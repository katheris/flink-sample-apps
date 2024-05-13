package flink.realtimeanalytics;

import flink.datasource.KafkaSourceDataGenerator;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

public class RetailSalesStreamingOperations {
    public static void main(String[] args) {

        try {

            StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            streamEnv.setParallelism(3);

            String dataDir = "data/products.csv";
            final FileSource<String> fileSrc =
                    FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(dataDir))
                            .build();
            final DataStream<Product> productsObj =
                    streamEnv.fromSource(fileSrc, WatermarkStrategy.noWatermarks(), "products")
                            .map(new MapFunction<String, Product>() {
                                @Override
                                public Product map(String value) throws Exception {
                                    System.out.println("--- Read product : " + value);
                                    return new Product(value);
                                }
                            });



            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            properties.setProperty("group.id", "flink.realtime.analytics");

            KafkaSource<String> kafkaSrc = KafkaSource.<String>builder()
                    .setBootstrapServers("localhost:9092")
                    .setTopics("flink.sales.records")
                    .setGroupId("flink.realtime.analytics")
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            DataStream<SalesRecord> salesRecords =
                    streamEnv.fromSource(kafkaSrc, WatermarkStrategy.noWatermarks(), "KafkaSource")
                            .map(new MapFunction<String, SalesRecord>() {
                @Override
                public SalesRecord map(String value) throws Exception {
                    System.out.println("--- Read sales record : " + value);
                    return new SalesRecord(value);
                }
            });

            KeyedStream<SalesRecord,Integer> salesByProduct = salesRecords
                    .keyBy((KeySelector<SalesRecord, Integer>) sale -> sale.productId);

            MapStateDescriptor<Integer, Product> productMapStateDescriptor = new MapStateDescriptor<Integer, Product>(
                    "ProductCategory",
                    Types.INT,
                    Types.GENERIC(Product.class)
            );

            BroadcastStream<Product> productBroadcastStream = productsObj.broadcast(productMapStateDescriptor);

            DataStream<Tuple2<String,Long>> matches = salesByProduct
                    .connect(productBroadcastStream)
                    .process(new KeyedBroadcastProcessFunction<Integer, SalesRecord, Product, Tuple2<String,Long>>() {

                         private final MapStateDescriptor<Integer,Product> productDesc =
                                 new MapStateDescriptor<>(
                                 "ProductCategory",
                                         Types.INT,
                                         Types.GENERIC(Product.class)
                                );

                        @Override
                        public void processElement(SalesRecord salesRecord, KeyedBroadcastProcessFunction<Integer, SalesRecord, Product, Tuple2<String, Long>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<String, Long>> collector) throws Exception {
                            Product product = readOnlyContext.getBroadcastState(productDesc).get(salesRecord.productId);

                            if (product != null) {
                                System.out.println("- Sale for category " + product.category + ": " + salesRecord.getQuantity());
                                collector.collect(new Tuple2<String,Long>(product.category, salesRecord.getQuantity()));
                            }

                        }

                        @Override
                        public void processBroadcastElement(Product product, KeyedBroadcastProcessFunction<Integer, SalesRecord, Product, Tuple2<String, Long>>.Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
                            context.getBroadcastState(productDesc).put(product.productId, product);
                        }
                    });

            DataStream<Tuple2<String,Long>> salesByCategory = matches.keyBy(v -> v.f0)
                    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                    .reduce(new ReduceFunction<Tuple2<String,Long>>() {

                        @Override
                        public Tuple2<String, Long> reduce(Tuple2<String, Long> stringIntegerTuple2, Tuple2<String, Long> t1) throws Exception {
                            return new Tuple2<String, Long>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
                        }
                    });

            salesByCategory.print();

            System.out.println("Starting sales data generator");
            Thread kafkaThread = new Thread(new KafkaSourceDataGenerator());
            kafkaThread.start();

            streamEnv.execute("Computing sales by category for every hour");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}