package kafka;

import kafka.schema.Sales;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;

public class SalesDataGeneratorAvro implements Runnable {
    final String bootstrapServers;
    final String registryUrl;
    public SalesDataGeneratorAvro(String bootstrapServers, String registryUrl) {
        this.bootstrapServers = bootstrapServers;
        this.registryUrl = registryUrl;
    }

    public static void main(String[] args) {
        SalesDataGeneratorAvro ksdg = new SalesDataGeneratorAvro(args[0], args[1]);
        ksdg.run();
    }

    @Override
    public void run() {
        Producer<String, Object> producer = new KafkaProducer<>(KafkaClientProps.avro(bootstrapServers, registryUrl));

        try {

            Random random = new Random();

            //Generate 100 sample sale records
            while (true) {

                String userId = "user-" + Math.abs(random.nextInt(100));

                String invoiceId = String.valueOf(Math.abs(random.nextLong()));

                String productId = String.valueOf(Math.abs(random.nextInt(200)));

                String quantity = String.valueOf(Math.abs(random.nextInt(3) + 1));

                String cost = "£" + Math.abs(random.nextInt(1000) + 1);

                String key = String.valueOf(System.currentTimeMillis());
                GenericRecord avroRecord = new GenericData.Record(Sales.SCHEMA$);
                avroRecord.put("user_id", userId);
                avroRecord.put("product_id", productId);
                avroRecord.put("invoice_id", invoiceId);
                avroRecord.put("quantity", quantity);
                avroRecord.put("unit_cost", cost);
                ProducerRecord<String, Object> record =
                        new ProducerRecord<>(
                                SalesDataGenerator.TOPIC,
                                key,
                                avroRecord);

                producer.send(record).get();

                System.out.println("Kafka Sales Data Generator : Sent Event with : "
                        + "userId: " + userId + " productId: " + productId);

                //Sleep for a random time ( 1 - 3 secs) before the next record.
                Thread.sleep(random.nextInt(2000) + 1);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }
    }
}
