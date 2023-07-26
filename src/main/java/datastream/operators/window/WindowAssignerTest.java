package datastream.operators.window;

import common.test.KeywordEvent;
import common.test.KeywordEventBasicFunctions;
import demo.functions.WatermarkPrintSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WindowAssignerTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.port", "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .map(new KeywordEventBasicFunctions.SocketText2KeywordEvent())
//                .setParallelism(1)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<KeywordEvent>forGenerator(new KeywordEventBasicFunctions.PunctuatedWatermarkGeneratorSupplier<>(Duration.ofSeconds(2L)))
                                .withTimestampAssigner(new KeywordEventBasicFunctions.keywordEventTsExtractor())
//                                .withIdleness()
                )
                .disableChaining()
                .addSink(new WatermarkPrintSink<>());

        env.execute("Window Assigner测试");
    }
}
