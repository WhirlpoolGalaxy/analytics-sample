package sample;

public class Purchase {
    private String orderId;
    private String productName;
    private double amount;

    public Purchase(String orderId, String productName, double amount) {
        this.orderId = orderId;
        this.productName = productName;
        this.amount = amount;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getProductName() {
        return productName;
    }

    public double getAmount() {
        return amount;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }
}
