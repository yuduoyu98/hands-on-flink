package demo;

import demo.bean.taxi.datatypes.TaxiFare;
import demo.bean.taxi.sources.TaxiFareGenerator;
import demo.functions.WatermarkPrintSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class EventTimeAndWatermarkAssign {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new TaxiFareGenerator())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        (fare, t) -> fare.getEventTimeMillis()))
                .addSink(new WatermarkPrintSink<>());

        env.execute();

    }
}
