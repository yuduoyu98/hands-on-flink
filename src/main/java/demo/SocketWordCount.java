package demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //windows: nc -lp 9999
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);

        socketDS.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (text, collector) -> {
                    for (String word : text.split(" ")) {
                        collector.collect(Tuple2.of(word, 1L));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
