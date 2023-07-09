package demo;

import demo.bean.Event;
import demo.functions.RandomEventTsSource;
import demo.functions.WatermarkPrintSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class EventTimeAndWatermarkAssign {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(
                        new RandomEventTsSource(
                                RandomEventTsSource.Level.LEVEL_1,
                                RandomEventTsSource.Level.LEVEL_3,
                                RandomEventTsSource.Level.LEVEL_2))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        (event, ts) -> event.getTimestamp()))
                .addSink(new WatermarkPrintSink<>());

        env.execute();

    }
}
