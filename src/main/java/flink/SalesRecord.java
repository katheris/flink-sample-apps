package flink;

public class SalesRecord {
    long invoiceId;
    int userId;
    int productId;
    long quantity;
    int unitCost;

    public SalesRecord(String salesRecord) {
        //Split the string
        String[] attributes = salesRecord
                .replace("\"","")
                .split(",");

        this.invoiceId = Long.valueOf(attributes[0]);
        this.userId = Integer.valueOf(attributes[1]);
        this.productId = Integer.valueOf(attributes[2]);
        this.quantity = Long.valueOf(attributes[3]);
        this.unitCost = Integer.valueOf(attributes[4]);
    }

    public long getInvoiceId() {
        return invoiceId;
    }

    public int getUserId() {
        return userId;
    }

    public int getProductId() {
        return productId;
    }

    public long getQuantity() {
        return quantity;
    }

    public int getUnitCost() {
        return unitCost;
    }


    @Override
    public String toString() {
        return "RetailSalesRecord{" +
                "invoiceId=" + invoiceId +
                ", userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", quantity='" + quantity + '\'' +
                ", unitCost=" + unitCost +
                '}';
    }
}
