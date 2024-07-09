package kafka;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ClickStreamDataGenerator implements Runnable {
    final public static String TOPIC = "flink.click.streams";
    final String bootstrapServer;
    public ClickStreamDataGenerator(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public static void main(String[] args) {
        ClickStreamDataGenerator csdg = new ClickStreamDataGenerator(args[0]);
        csdg.run();
    }

    @Override
    public void run() {
        Random random = new Random();
        Producer<String,String> producer = new KafkaProducer<>(KafkaClientProps.csv(bootstrapServer));

        try {

            while (true) {
                String productId = String.valueOf(Math.abs(random.nextInt(200)));

                String userId = "user-" + Math.abs(random.nextInt(100));

                String[] recordInCSV = {userId, productId};

                String key = String.valueOf(System.currentTimeMillis());
                ProducerRecord<String, String> record =
                        new ProducerRecord<String,String>(
                                TOPIC,
                                key,
                                String.join(",", recordInCSV)  );

                producer.send(record).get();

                System.out.println("Kafka Click Stream Generator : Sent Event with : " +
                        "userId: " + recordInCSV[0] + " productId: " + recordInCSV[1]);

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

