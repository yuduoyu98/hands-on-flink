package common.test;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Duration;

import static common.utils.TimeUtils.formatTs;

public class KeywordEventBasicFunctions {

    /**
     * <tt>socket</tt>文本转<tt>bean</tt>：{@link KeywordEvent KeywordEvenet}
     */
    public static class SocketText2KeywordEvent implements MapFunction<String, KeywordEvent> {
        @Override
        public KeywordEvent map(String text) {
            String[] splits = text.split(",");
            return new KeywordEvent(splits[0], Long.valueOf(splits[1]), Long.valueOf(splits[2]));
        }
    }

    /**
     * {@link KeywordEvent KeywordEvenet}的事件时间提取器
     */
    public static class keywordEventTsExtractor implements SerializableTimestampAssigner<KeywordEvent> {
        @Override
        public long extractTimestamp(KeywordEvent element, long recordTimestamp) {
            return element.ts;
        }
    }

    /**
     * {@link WatermarkStrategy#forGenerator(WatermarkGeneratorSupplier)} 入参接口实现
     * {@link KeywordEventBasicFunctions.PunctuatedWatermarkGenerator PunctuatedWatermarkGenerator}的<tt>supplier</tt>
     * @param <T>
     */
    public static class PunctuatedWatermarkGeneratorSupplier<T> implements WatermarkGeneratorSupplier<T> {

        private final Duration maxOutOfOrderness;

        public PunctuatedWatermarkGeneratorSupplier(Duration maxOutOfOrderness){
            this.maxOutOfOrderness = maxOutOfOrderness;
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(Context context) {
            return new PunctuatedWatermarkGenerator<>(maxOutOfOrderness);
        }
    }

    /**
     * <strong><tt>watermark</tt>生成器</strong>：逐个事件尝试<tt>emit watermark</tt>（如果是<tt>maxTimestamp</tt>）
     * <p> 1. 支持提供一个乱序时间上限
     * <p> 2. 会打印watermark生成信息</p>
     */
    public static class PunctuatedWatermarkGenerator<T> implements WatermarkGenerator<T> {

        private long maxTimestamp = Long.MIN_VALUE;
        private final long maxOutOfOrderness;

        public PunctuatedWatermarkGenerator(Duration maxOutOfOrderness){
            this.maxOutOfOrderness = maxOutOfOrderness.toMillis();
        }

        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            if (eventTimestamp > maxTimestamp) {
                maxTimestamp = eventTimestamp;
                long watermarkTs = maxTimestamp - maxOutOfOrderness - 1;
                output.emitWatermark(new Watermark(watermarkTs));
                StringBuilder sb = new StringBuilder();
                sb.append("WatermarkGenerator").append("> ");
                sb.append("当前线程ID：").append(Thread.currentThread().getId()).append(",");
                sb.append("事件时间：[").append(eventTimestamp).append("|").append(formatTs(eventTimestamp)).append("],");
                sb.append("生成水位线：[").append(watermarkTs).append("|").append(formatTs(watermarkTs)).append("],");
                sb.append("事件：").append(event);
                System.out.println(sb);
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //do nothing
        }
    }

}
