package flink.realtimeanalytics;

public class Product {
    int productId;

    String category;

    public Product(String product) {
        String[] attributes = product
                .replace("\"","")
                .split(",");

        this.productId = Integer.valueOf(attributes[0]);
        this.category = attributes[1];
    }

    @Override
    public String toString() {
        return "Product{" +
                ", productId='" + productId + '\'' +
                ", category=" + category +
                '}';
    }
}
