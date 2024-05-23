package flink.recommendationApp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProductInventoryDataGenerator {

    public static void generateProductData() throws IOException, ExecutionException, InterruptedException {

        //Create Kafka Client
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<String, String>(props);

        List<String> productCategories = new ArrayList<>();
        productCategories.add("technology");
        productCategories.add("home&furniture");
        productCategories.add("garden");
        productCategories.add("appliances");
        productCategories.add("toys");
        productCategories.add("gaming");
        productCategories.add("clothing");
        productCategories.add("accessories");
        productCategories.add("health&beauty");
        productCategories.add("sports&leisure");

        Random random = new Random();


        for(int i=0; i < 200; i++) {
            String productCategory = productCategories.get(random.nextInt(productCategories.size()));

            String stock = String.valueOf(1000);

            String rating = String.valueOf(Math.abs(random.nextInt(10) + 1));

            String[] recordInCSV = {productCategory, String.valueOf(i), stock, rating};


            ProducerRecord<String, String> record =
                    new ProducerRecord<String,String>(
                            "flink.product.inventory",
                            "",
                            String.join(",", recordInCSV)  );

            producer.send(record).get();

            System.out.println("Kafka Product Inventory Data Generator : Sending Event : "
                    + String.join(",", recordInCSV));
        }
    }
}
