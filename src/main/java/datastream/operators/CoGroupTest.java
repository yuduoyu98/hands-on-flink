package datastream.operators;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class CoGroupTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        List<Tuple3<String, Long, Integer>> elements1 = Arrays.asList(
                Tuple3.of("hello", 1L, 1),
                Tuple3.of("world", 1L, 3),
                Tuple3.of("hello", 1L, 5),
                Tuple3.of("flink", 3L, 7),
                Tuple3.of("java", 1L, 9)
        );

        List<Tuple3<String, Long, Integer>> elements2 = Arrays.asList(
                Tuple3.of("scala", 1L, 2),
                Tuple3.of("world", 2L, 4),
                Tuple3.of("hello", 1L, 6),
                Tuple3.of("flink", 1L, 8),
                Tuple3.of("java", 1L, 10)
        );

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> ds1 = env
                .fromCollection(elements1)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, Long, Integer>>) (tuple, recordTimestamp) -> tuple.f2 * 1000L));

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> ds2 = env.fromCollection(elements2)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, Long, Integer>>) (tuple, recordTimestamp) -> tuple.f2 * 1000L));
        ds1
                .coGroup(ds2)
                .where(tuple -> tuple.f0)
                .equalTo(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(11)))
                .apply(new CoGroupFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, String>() {

                    @Override
                    public void coGroup(
                            Iterable<Tuple3<String, Long, Integer>> first,
                            Iterable<Tuple3<String, Long, Integer>> second,
                            Collector<String> out) throws Exception {
                        StringBuilder sb = new StringBuilder();
                        sb.append("[");
                        for (Tuple3<String, Long, Integer> tuple : first) {
                            sb.append(tuple.f0).append(",");
                        }
                        sb.append("]\n");
                        sb.append("[");
                        for (Tuple3<String, Long, Integer> tuple : second) {
                            sb.append(tuple.f0).append(",");
                        }
                        sb.append("]\n");
                        out.collect(sb.toString());
                    }
                })
                .print();
        env.execute();
    }
}
