package datastream.operators.window;

import common.test.KeywordEvent;
import common.test.KeywordEventBasicFunctions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class SumTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .socketTextStream("localhost", 9999)
                .map(new KeywordEventBasicFunctions.SocketText2KeywordEvent())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<KeywordEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                                .withTimestampAssigner(new KeywordEventBasicFunctions.keywordEventTsExtractor())
//                                .withIdleness()
                )
                .keyBy(KeywordEvent::getWord)
                //sum: 如果为Bean必须为POJO；可以填POJO字段名；只能sum一个字段
//                .sum("cnt")
                .reduce(new ReduceFunction<KeywordEvent>() {
                    @Override
                    public KeywordEvent reduce(KeywordEvent value1, KeywordEvent value2) throws Exception {
                        return new KeywordEvent(value1.word, value1.cnt + value2.cnt, Math.max(value1.ts, value2.ts));
                    }
                })
                .print();

        env.execute();
    }
}
