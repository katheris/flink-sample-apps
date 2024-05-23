package flink.recommendationApp;

import flink.common.SalesDataGenerator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

public class RetailRecommendationApp {
    private static final String SALES_RECORDS_TOPIC = "flink.sales.records";
    private static final String CLICK_STREAMS_TOPIC = "flink.click.streams";
    private static final String RECOMMENDATION_TOPIC = "flink.recommendations";

    public static void main(String[] args) {

        try {

            EnvironmentSettings settings = EnvironmentSettings
                    .newInstance()
                    .inStreamingMode()
                    .build();

            TableEnvironment tableEnv = TableEnvironment.create(settings);
            tableEnv.getConfig().set("parallelism.default", "1");

            final Schema clickStreamSchema = Schema.newBuilder()
                    .column("user_id", DataTypes.STRING())
                    .column("product_id", DataTypes.STRING())
                    .columnByMetadata("click_ts", DataTypes.TIMESTAMP(3), "timestamp")
                    .build();
            tableEnv.createTemporaryTable("ClickStreamTable", TableDescriptor.forConnector("kafka")
                    .schema(clickStreamSchema)
                    .option("topic", CLICK_STREAMS_TOPIC)
                    .option("properties.bootstrap.servers","localhost:9092")
                    .option("properties.group.id", "flink.recommendation.app")
                    .option("scan.startup.mode","latest-offset")
                    .format("csv")
                    .build());

            final Schema productInventorySchema = Schema.newBuilder()
                    .column("product_id", DataTypes.STRING())
                    .column("category", DataTypes.STRING())
                    .column("stock", DataTypes.STRING())
                    .column("rating", DataTypes.STRING())
                    .build();
            tableEnv.createTemporaryTable("ProductInventoryTable", TableDescriptor.forConnector("filesystem")
                    .schema(productInventorySchema)
                    .option("path","data/productInventory.csv")
                    .format("csv")
                    .option("csv.ignore-parse-errors","true")
                    .build());

            final Schema salesRecordSchema = Schema.newBuilder()
                    .column("invoice_id", DataTypes.STRING())
                    .column("user_id", DataTypes.STRING())
                    .column("product_id", DataTypes.STRING())
                    .column("quantity", DataTypes.STRING())
                    .column("unitCost", DataTypes.STRING())
                    .build();
            tableEnv.createTemporaryTable("SalesRecordTable", TableDescriptor.forConnector("kafka")
                    .schema(salesRecordSchema)
                    .option("topic", SALES_RECORDS_TOPIC)
                    .option("properties.bootstrap.servers","localhost:9092")
                    .option("properties.group.id", "flink.recommendation.app")
                    .option("scan.startup.mode","latest-offset")
                    .format("csv")
                    .build());

            final Schema outputSchema = Schema.newBuilder()
                    .column("user_id", DataTypes.STRING().notNull())
                    .column("product_id", DataTypes.STRING())
                    .column("rating", DataTypes.STRING())
                    .primaryKey("user_id")
                    .build();

            tableEnv.createTable("CsvSinkTable", TableDescriptor.forConnector("upsert-kafka")
                    .schema(outputSchema)
                    .option("topic", RECOMMENDATION_TOPIC)
                    .option("properties.bootstrap.servers","localhost:9092")
                    .option("key.format", "csv")
                    .option("value.format", "csv")
                    .option("value.fields-include", "ALL")
                    .build());


            Table recommendation = tableEnv.sqlQuery("WITH ClickedProducts AS (\n" +
                    "    SELECT DISTINCT\n" +
                    "        cs.user_id,\n" +
                    "        cs.click_ts,\n" +
                    "        pi.category\n" +
                    "    FROM\n" +
                    "        ClickStreamTable cs\n" +
                    "    JOIN\n" +
                    "        ProductInventoryTable pi ON cs.product_id = pi.product_id\n" +
                    "),\n" +
                    "CategoryProducts AS (\n" +
                    "    SELECT\n" +
                    "         cp.user_id,\n" +
                    "         cp.click_ts,\n" +
                    "         pi2.product_id,\n" +
                    "         pi2.category,\n" +
                    "         pi2.stock,\n" +
                    "         pi2.rating,\n" +
                    "         sr.user_id AS purchased\n" +
                    "     FROM\n" +
                    "         ClickedProducts cp\n" +
                    "     JOIN\n" +
                    "         ProductInventoryTable pi2 ON cp.category = pi2.category\n" +
                    "     LEFT JOIN\n" +
                    "         SalesRecordTable sr ON cp.user_id = sr.user_id AND pi2.product_id = sr.product_id\n" +
                    "     WHERE pi2.stock > 0\n" +
                    "     GROUP BY \n" +
                    "         pi2.product_id, \n" +
                    "         pi2.category, \n" +
                    "         pi2.stock, \n" +
                    "         cp.user_id, \n" +
                    "         cp.click_ts, \n" +
                    "         sr.user_id, \n" +
                    "         pi2.rating \n" +
                    "),\n" +
                    "RankedProducts AS (\n" +
                    "    SELECT\n" +
                    "         cp.user_id,\n" +
                    "         cp.click_ts,\n" +
                    "         cp.product_id,\n" +
                    "         cp.category,\n" +
                    "         cp.stock,\n" +
                    "         cp.rating,\n" +
                    "         cp.purchased,\n" +
                    "         TUMBLE_END(click_ts, INTERVAL '5' SECOND),\n" +
                    "         ROW_NUMBER() OVER (PARTITION BY cp.user_id ORDER BY cp.purchased DESC, cp.rating DESC) AS rn\n" +
                    "    FROM\n" +
                    "         CategoryProducts cp\n" +
                    ")\n" +
                    "SELECT\n" +
                    "    user_id,\n" +
                    "    product_id,\n" +
                    "    rating\n" +
                    "FROM\n" +
                    "    RankedProducts\n" +
                    "WHERE\n" +
                    "    rn = 1"
            );

            Properties props = new Properties();
            props.put("bootstrap.servers","localhost:9092");

            Admin admin = Admin.create(props);
            Map<String, String> topicConfigs = new HashMap<>();
            topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            var outputTopic = new NewTopic(RECOMMENDATION_TOPIC, 1, (short) 1).configs(topicConfigs);
            admin.createTopics(Collections.singleton(outputTopic));

            System.out.println("Starting File Data Generator...");
            Thread genThread = new Thread(new ClickStreamDataGenerator());
            genThread.start();

            System.out.println("Starting Sales Data Generator");
            Thread kafkaThread = new Thread(new SalesDataGenerator());
            kafkaThread.start();

            TableResult tableResult = recommendation.insertInto("CsvSinkTable").execute();
            tableResult.print();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}






