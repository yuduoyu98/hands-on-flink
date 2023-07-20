package datastream.watermark;

import common.bean.taxi.datatypes.TaxiFare;
import common.bean.taxi.sources.TaxiFareGenerator;
import demo.functions.WatermarkPrintSink;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new TaxiFareGenerator())
                //1. Common Watermark Strategy: Fixed Amount of Lateness -> 周期性的emit水位线(=最大事件时间戳-maxOutOfOrderness-1)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((fare, t) -> fare.getEventTimeMillis())
                )
                //2. Common Watermark Strategy: Monotonously Increasing Timestamps -> 特殊的Bounded-out-of-orderness: maxOutOfOrderness=0
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                                .withTimestampAssigner((fare, t) -> fare.getEventTimeMillis())
                )
                //3. Periodic WatermarkGenerator（自定义）
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier<TaxiFare>() {
                    @Override
                    public WatermarkGenerator<TaxiFare> createWatermarkGenerator(Context context) {
                        return new TimeLagWatermarkGenerator(Duration.ofSeconds(5));
                    }
                }))
                //4. Punctuated WatermarkGenerator (自定义)
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(new WatermarkGeneratorSupplier<TaxiFare>() {
                    @Override
                    public WatermarkGenerator<TaxiFare> createWatermarkGenerator(Context context) {
                        return new PunctuatedWatermarkGenerator<>();
                    }
                }))
                .addSink(new WatermarkPrintSink<>());

        env.execute();
    }

    /**
     * This generator generates watermarks that are lagging behind processing time
     * by a fixed amount. It assumes that elements arrive in Flink after a bounded delay.
     */
    public static class TimeLagWatermarkGenerator implements WatermarkGenerator<TaxiFare> {

        private final long maxLag;

        public TimeLagWatermarkGenerator(Duration lag) {
            maxLag = lag.toMillis();
        }

        @Override
        public void onEvent(TaxiFare event, long eventTimestamp, WatermarkOutput output) {
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(System.currentTimeMillis() - maxLag));
        }
    }

    public static class PunctuatedWatermarkGenerator<T> implements WatermarkGenerator<T>{

        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            //不建议每个event都生成watermark 大量的watermark会让性能降级
            output.emitWatermark(new Watermark(eventTimestamp));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //不需要 这个方法为系统调用 周期生成watermark
        }
    }
}
