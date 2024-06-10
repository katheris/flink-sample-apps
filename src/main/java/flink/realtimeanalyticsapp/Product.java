package flink.realtimeanalyticsapp;

public class Product {
    int productId;

    String category;

    int stock;

    int rating;


    public Product(String product) {
        String[] attributes = product
                .replace("\"","")
                .split(",");

        this.productId = Integer.valueOf(attributes[0]);
        this.category = attributes[1];
        this.stock = Integer.parseInt(attributes[2]);
        this.rating = Integer.parseInt(attributes[3]);

    }

    @Override
    public String toString() {
        return "Product{" +
                ", productId='" + productId + '\'' +
                ", category=" + category +
                ", stock=" + stock +
                ", rating=" + rating +
                '}';
    }
}
