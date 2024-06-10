package flink.realtimeanalyticsapp;


public final class SalesByCategory {

    private String category;

    private Long sales;

    public SalesByCategory(String category, Long sales) {
        this.category = category;
        this.sales = sales;
    }


    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Long getSales() {
        return sales;
    }

    public void setSales(Long sales) {
        this.sales = sales;
    }

    @Override
    public String toString() {
        return "SalesByCategory" +
                "{category=" + category +
                ", sales=" + sales + "}";
    }
}
