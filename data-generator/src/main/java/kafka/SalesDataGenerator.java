package kafka;

import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SalesDataGenerator implements Runnable {
    final public static String TOPIC = "flink.sales.records";
    final String bootstrapServers;
    public SalesDataGenerator(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public static void main(String[] args) {
        SalesDataGenerator ksdg = new SalesDataGenerator(args[0]);
        ksdg.run();
    }

    @Override
    public void run() {
        Producer<String,String> producer = new KafkaProducer<>(KafkaClientProps.csv(bootstrapServers));

        try {

            Random random = new Random();

            //Generate 100 sample sale records
            while (true) {

                String userId = "user-" + Math.abs(random.nextInt(100));

                String invoiceId = String.valueOf(Math.abs(random.nextLong()));

                String productId = String.valueOf(Math.abs(random.nextInt(200)));

                String quantity = String.valueOf(Math.abs(random.nextInt(3) + 1));

                String cost = "£" + Math.abs(random.nextInt(1000) + 1);

                String[] recordInCSV = {invoiceId, userId, productId,
                        quantity, cost};


                String key = String.valueOf(System.currentTimeMillis());
                ProducerRecord<String, String> record =
                        new ProducerRecord<String,String>(
                                TOPIC,
                                key,
                                String.join(",", recordInCSV)  );

                producer.send(record).get();

                System.out.println("Kafka Sales Data Generator : Sent Event with : "
                        + "userId: " + recordInCSV[1] + " productId: " + recordInCSV[2]);

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
