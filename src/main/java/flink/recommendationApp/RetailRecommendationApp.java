package flink.recommendationApp;

import flink.common.SalesDataGenerator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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

/*
Recommendation system use case

Example: Retail Sales

The scenario:
    Orinoco Inc wants to display a widget on product pages of similar products that the user might be interested in buying.
    They should recommend:
        - Up to 6 highly rated products in the same category as that of the product the customer is currently viewing
        - Only products that are in stock
        - Products that the customer has bought before should be favoured
        - TODO: Avoid showing suggestions that have already been made in previous pageviews

The data:
    - Input: A clickstream (user id, product id, event time)
    - Input: A stream of purchases (user id, product id, purchase date)
    - Input: An inventory of products (product id, product name, product category, number in stock, rating)

Output: A stream of recommendations (user id, 6 product ids)
*/

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
                    .columnByMetadata("event_time", DataTypes.TIMESTAMP(3), "timestamp")
                    .watermark("event_time", "event_time - INTERVAL '1' SECOND")
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
                    .option("path", getFileFromResource("productInventory.csv"))
                    .format("csv")
                    .option("csv.ignore-parse-errors","true")
                    .build());

            final Schema salesRecordSchema = Schema.newBuilder()
                    .column("invoice_id", DataTypes.STRING())
                    .column("user_id", DataTypes.STRING())
                    .column("product_id", DataTypes.STRING())
                    .column("quantity", DataTypes.STRING())
                    .column("unit_cost", DataTypes.STRING())
                    .columnByMetadata("purchase_time", DataTypes.TIMESTAMP(3), "timestamp", true)
                    .watermark("purchase_time", "purchase_time - INTERVAL '1' SECOND")
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
                    .column("top_product_ids", DataTypes.STRING())
                    .column("event_time", DataTypes.TIMESTAMP(3))
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


            //create a view of clicked products with category and click event time
            tableEnv.executeSql("CREATE TEMPORARY VIEW clicked_products AS\n" +
                    "SELECT DISTINCT\n" +
                    "    c.user_id,\n" +
                    "    c.event_time ,\n" +
                    "    p.product_id,\n" +
                    "    p.category\n" +
                    "FROM ClickStreamTable AS c\n" +
                    "JOIN ProductInventoryTable AS p ON c.product_id = p.product_id;");

            //create a view of stocked and purchased products that are in the same category as the clicked products
            tableEnv.executeSql("CREATE TEMPORARY VIEW category_products AS\n" +
                    "SELECT\n" +
                    "   cp.user_id,\n" +
                    "   cp.event_time,\n" +
                    "   p.product_id,\n" +
                    "   p.category,\n" +
                    "   p.stock,\n" +
                    "   p.rating,\n" +
                    "   sr.user_id as purchased\n" +
                    "FROM clicked_products cp\n" +
                    "JOIN ProductInventoryTable AS p ON cp.category = p.category\n" +
                    "LEFT JOIN SalesRecordTable sr ON cp.user_id = sr.user_id AND p.product_id = sr.product_id\n" +
                    "WHERE p.stock > 0\n" +
                    "GROUP BY \n" +
                    "    p.product_id,\n" +
                    "    p.category, \n" +
                    "    p.stock, \n" +
                    "    cp.user_id, \n" +
                    "    cp.event_time, \n" +
                    "    sr.user_id, \n" +
                    "    p.rating;");

            //create a view of products ordered by the rating
            tableEnv.executeSql("CREATE TEMPORARY VIEW top_products AS\n" +
                    "    SELECT\n" +
                    "         cp.user_id,\n" +
                    "         cp.event_time,\n" +
                    "         cp.product_id,\n" +
                    "         cp.category,\n" +
                    "         cp.stock,\n" +
                    "         cp.rating,\n" +
                    "         cp.purchased,\n" +
                    "         ROW_NUMBER() OVER (PARTITION BY cp.user_id ORDER BY cp.purchased DESC, cp.rating DESC) AS rn\n" +
                    "    FROM\n" +
                    "         category_products cp;");

            //create a table of top 5 rated products, grouped by user and 5 seconds of window
            // the output should be user_id with product ids separated by comma and window timestamp
            Table recommendation = tableEnv.sqlQuery("SELECT\n" +
                    "    user_id,\n" +
                    "    LISTAGG(product_id, ',') AS top_product_ids,\n" +
                    "    TUMBLE_END(event_time, INTERVAL '5' SECOND)\n" +
                    "FROM top_products\n" +
                    "WHERE rn <= 6\n" +
                    "GROUP BY \n" +
                    "    user_id,\n" +
                    "    TUMBLE(event_time, INTERVAL '5' SECOND);");

            Properties props = new Properties();
            props.put("bootstrap.servers","localhost:9092");

            Admin admin = Admin.create(props);
            Map<String, String> topicConfigs = new HashMap<>();
            topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            var outputTopic = new NewTopic(RECOMMENDATION_TOPIC, 1, (short) 1).configs(topicConfigs);
            admin.createTopics(Collections.singleton(outputTopic));

            System.out.println("Starting Click Stream Data Generator...");
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

    private static String getFileFromResource(String filename) {
        return Objects.requireNonNull(RetailRecommendationApp.class.getClassLoader().getResource(filename)).getPath();
    }
}






