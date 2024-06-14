package flink.realtimeanalyticsapp;

import com.fasterxml.jackson.databind.ObjectMapper;
import flink.common.SalesDataGenerator;
import flink.common.SalesRecord;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Objects;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*
Realtime Analytics Use Case

Example: Retail Sales

The scenario: Orinoco Inc wants a realtime dashboard of cumulative sales figures over the last hour, broken down by product category.

The data:
    - Input: A stream of sales (invoice id, user id, product id, quantity, unit cost)
    - Input: A stream of products (product id, product category)

Output: Product sales (product category, sales)
*/

public class RetailSalesStreamingOperations {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper() ;
    private static final String SALES_RECORDS_TOPIC = "flink.sales.records";
    private static final String SALES_BY_CATEGORY_TOPIC = "flink.sales.by.category";

    public static void main(String[] args) {

        try {

            StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            streamEnv.setParallelism(3);

            final FileSource<String> fileSrc =
                    FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(getFileFromResource("productInventory.csv")))
                            .build();
            final DataStream<Product> productsObj =
                    streamEnv.fromSource(fileSrc, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)), "products")
                            .map(new MapFunction<String, Product>() {
                                @Override
                                public Product map(String value) throws Exception {
                                    System.out.println("--- Read product : " + value);
                                    return new Product(value);
                                }
                            });

            String bootstrapServers = "localhost:9092";

            KafkaSource<String> kafkaSrc = KafkaSource.<String>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setTopics(SALES_RECORDS_TOPIC)
                    .setGroupId("flink.realtime.analytics")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            DataStream<SalesRecord> salesRecords =
                    streamEnv.fromSource(kafkaSrc, WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)), "KafkaSource")
                            .map(new MapFunction<String, SalesRecord>() {
                @Override
                public SalesRecord map(String value) throws Exception {
                    System.out.println("--- Read sales record : " + value);
                    return new SalesRecord(value);
                }
            });

            KeyedStream<SalesRecord,Integer> salesByProduct = salesRecords
                    .keyBy((KeySelector<SalesRecord, Integer>) sale -> sale.getProductId());

            MapStateDescriptor<Integer, Product> productMapStateDescriptor = new MapStateDescriptor<Integer, Product>(
                    "ProductCategory",
                    Types.INT,
                    Types.GENERIC(Product.class)
            );

            BroadcastStream<Product> productBroadcastStream = productsObj.broadcast(productMapStateDescriptor);

            DataStream<SalesByCategory> matches = salesByProduct
                    .connect(productBroadcastStream)
                    .process(new KeyedBroadcastProcessFunction<Integer, SalesRecord, Product, SalesByCategory>() {

                         private final MapStateDescriptor<Integer,Product> productDesc =
                                 new MapStateDescriptor<>(
                                 "ProductCategory",
                                         Types.INT,
                                         Types.GENERIC(Product.class)
                                );

                        @Override
                        public void processElement(SalesRecord salesRecord, KeyedBroadcastProcessFunction<Integer, SalesRecord, Product, SalesByCategory>.ReadOnlyContext readOnlyContext, Collector<SalesByCategory> collector) throws Exception {
                            Product product = readOnlyContext.getBroadcastState(productDesc).get(salesRecord.getProductId());

                            if (product != null) {
                                System.out.println("- Sale for category " + product.category + ": " + salesRecord.getQuantity());
                                collector.collect(new SalesByCategory(product.category, salesRecord.getQuantity()));
                            }
                        }

                        @Override
                        public void processBroadcastElement(Product product, KeyedBroadcastProcessFunction<Integer, SalesRecord, Product, SalesByCategory>.Context context, Collector<SalesByCategory> collector) throws Exception {
                            context.getBroadcastState(productDesc).put(product.productId, product);
                        }

                    });

            // For demo purpose, window is set to 10 seconds
            DataStream<SalesByCategory> salesByCategory = matches.keyBy(v -> v.getCategory())
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                    .reduce(new ReduceFunction<SalesByCategory>() {
                        @Override
                        public SalesByCategory reduce(SalesByCategory salesByCategory, SalesByCategory t1) throws Exception {
                            return new SalesByCategory(salesByCategory.getCategory(), salesByCategory.getSales() + t1.getSales());
                        }
                    });

            salesByCategory.print();

            KafkaSink<SalesByCategory> kafkaProducer = KafkaSink.<SalesByCategory>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .setProperty("transaction.timeout.ms", "800000")
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(SALES_BY_CATEGORY_TOPIC)
                            .setValueSerializationSchema((SerializationSchema<SalesByCategory>) element -> {
                                try {
                                    return OBJECT_MAPPER.writeValueAsBytes(element);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }).build()
                    )
                    .build();

            salesByCategory.sinkTo(kafkaProducer);

            System.out.println("Starting sales data generator");
            Thread kafkaThread = new Thread(new SalesDataGenerator());
            kafkaThread.start();

            streamEnv.execute("Computing sales by category for every hour");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getFileFromResource(String filename) {
        return Objects.requireNonNull(RetailSalesStreamingOperations.class.getClassLoader().getResource(filename)).getPath();
    }
}