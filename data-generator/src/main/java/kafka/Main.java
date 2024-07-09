package kafka;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        boolean useApicurioRegistry = Boolean.parseBoolean(System.getenv("USE_APICURIO_REGISTRY"));
        if (useApicurioRegistry) {
            String registryUrl = System.getenv("REGISTRY_URL");
            System.out.println("Starting Click Stream Data Generator with Avro data...");
            Thread csThread = new Thread(new ClickStreamDataGeneratorAvro(bootstrapServers, registryUrl));
            csThread.start();

            //So the two classes don't fight when registering the schemas in the registry
            Thread.sleep(5000);

            System.out.println("Starting Sales Data Generator with Avro data...");
            Thread kafkaThread = new Thread(new SalesDataGeneratorAvro(bootstrapServers, registryUrl));
            kafkaThread.start();
        } else {
            System.out.println("Starting Click Stream Data Generator...");
            Thread csThread = new Thread(new ClickStreamDataGenerator(bootstrapServers));
            csThread.start();

            System.out.println("Starting Sales Data Generator...");
            Thread kafkaThread = new Thread(new SalesDataGenerator(bootstrapServers));
            kafkaThread.start();
        }
    }
}