package entity;

public class BondDirectionData {

    private String direction;
    private float price;
    private String createTime;

    public BondDirectionData() {
    }

    public BondDirectionData(String direction, float price, String createTime) {
        this.direction = direction;
        this.price = price;
        this.createTime = createTime;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
