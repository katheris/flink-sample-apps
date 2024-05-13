package flink.datasource;

import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaSourceDataGenerator implements Runnable {

    public static void main(String[] args) {
        KafkaSourceDataGenerator ksdg = new KafkaSourceDataGenerator();
        ksdg.run();
    }

    @Override
    public void run() {

        try {

            //Create Kafka Client
            Properties props = new Properties();
            props.put("bootstrap.servers","localhost:9092");

            props.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String,String> producer = new KafkaProducer<String, String>(props);

            Random random = new Random();

            //Generate 100 sample sale records
            for(int i=0; i < 2000; i++) {

                String userId = String.valueOf(Math.abs(random.nextInt(500)));

                String invoiceId = String.valueOf(Math.abs(random.nextLong()));

                String productId = String.valueOf(Math.abs(random.nextInt(200)));

                String quantity = String.valueOf(Math.abs(random.nextInt(3) + 1));

                String cost = String.valueOf(Math.abs(random.nextInt(1000) + 1));

                String[] recordInCSV = {invoiceId, userId, productId,
                        quantity, cost};


                String key = String.valueOf(System.currentTimeMillis());
                ProducerRecord<String, String> record =
                        new ProducerRecord<String,String>(
                                "flink.sales.records",
                                key,
                                String.join(",", recordInCSV)  );

                RecordMetadata rmd = producer.send(record).get();

                System.out.println("Kafka Source Data Generator : Sending Event : "
                        + String.join(",", recordInCSV));

                //Sleep for a random time ( 1 - 3 secs) before the next record.
                Thread.sleep(random.nextInt(2000) + 1);

            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
