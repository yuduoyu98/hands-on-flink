package demo.bean;

import lombok.*;

import java.text.DecimalFormat;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Event {

    /**
     * 事件ID
     */
    private String id;

    /**
     * 事件时间
     */
    private long timestamp;

    /**
     * 销售额
     */
    private double sells;

    /**
     * 商品ID
     */
    private int skuID;

    private static DecimalFormat decimalFormat = new DecimalFormat("#.#");

    @Override
    public String toString() {
        return "Event{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", skuID=" + skuID +
                ", sells=" + decimalFormat.format(sells) +
                '}';
    }
}