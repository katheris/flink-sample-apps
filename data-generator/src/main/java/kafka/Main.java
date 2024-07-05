package kafka;

public class Main {
    public static void main(String[] args) {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");

        System.out.println("Starting Click Stream Data Generator...");
        Thread csThread = new Thread(new ClickStreamDataGenerator(bootstrapServers));
        csThread.start();

        System.out.println("Starting Sales Data Generator");
        Thread kafkaThread = new Thread(new SalesDataGenerator(bootstrapServers));
        kafkaThread.start();
    }
}