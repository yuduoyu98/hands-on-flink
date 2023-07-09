package demo;

import demo.bean.Event;
import demo.functions.RandomEventTsSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * window demo: 每小时被提及最多的关键词
 */
public class MostMentionedKeyWordPerHour {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(
                        new RandomEventTsSource(
                                RandomEventTsSource.Level.LEVEL_1,
                                RandomEventTsSource.Level.LEVEL_2,
                                RandomEventTsSource.Level.LEVEL_2))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        (event, ts) -> event.getTimestamp()))
                .map(event -> Tuple2.of(event.getKeyword(), (long) event.getCount()))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                }))
                .keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                //reduce入参和出参需要一致, aggregate/reduce无法拿到window的context -> 无法拿到窗口结束时间
                .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple3<Long, String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(
                            String word,
                            Context context,
                            Iterable<Tuple2<String, Long>> elements,
                            Collector<Tuple3<Long, String, Long>> out) {
                        long windowEndTs = context.window().getEnd();
                        long total = 0L;
                        for (Tuple2<String, Long> element : elements) {
                            total += element.f1;
                        }
                        out.collect(Tuple3.of(windowEndTs, word, total));
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .max(2)
                .print();


        env.execute();
    }
}
