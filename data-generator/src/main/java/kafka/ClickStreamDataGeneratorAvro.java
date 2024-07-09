package kafka;

import kafka.schema.ClickStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;
import java.util.concurrent.ExecutionException;

public class ClickStreamDataGeneratorAvro implements Runnable {
    final String bootstrapServer;
    final String registryUrl;
    public ClickStreamDataGeneratorAvro(String bootstrapServer, String registryUrl) {
        this.bootstrapServer = bootstrapServer;
        this.registryUrl = registryUrl;
    }

    public static void main(String[] args) {
        ClickStreamDataGeneratorAvro csdg = new ClickStreamDataGeneratorAvro(args[0], args[1]);
        csdg.run();
    }

    @Override
    public void run() {
        Random random = new Random();
        Producer<String, Object> producer = new KafkaProducer<>(KafkaClientProps.avro(bootstrapServer, registryUrl));

        try {

            while (true) {
                String productId = String.valueOf(Math.abs(random.nextInt(200)));

                String userId = "user-" + Math.abs(random.nextInt(100));

                String key = String.valueOf(System.currentTimeMillis());
                GenericRecord avroRecord = new GenericData.Record(ClickStream.SCHEMA$);
                avroRecord.put("user_id", userId);
                avroRecord.put("product_id", productId);
                ProducerRecord<String, Object> record =
                        new ProducerRecord<>(
                                ClickStreamDataGenerator.TOPIC,
                                key,
                                avroRecord
                        );

                producer.send(record).get();

                System.out.println("Kafka Click Stream Generator : Sent Event with : " +
                        "userId: " + userId + " productId: " + productId);

                Thread.sleep(3000);
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }
    }
}

