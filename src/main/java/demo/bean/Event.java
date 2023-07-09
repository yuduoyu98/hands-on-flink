package demo.bean;

import lombok.*;

/**
 *
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
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
     * 单句话提及次数
     */
    private int count;

    /**
     * 关键词
     */
    private String keyword;

}