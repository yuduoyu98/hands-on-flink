package demo;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceDS = env.fromElements("hello", "world", "hello", "flink");
        sourceDS.map(word -> Tuple2.of(word, 1L))
                //flink-java 在返回带有泛型的类型时 因为存在泛型擦除 需要returns显示指定返回类型
                //这些类型是必要的：Flink需根据类型在生成序列化/反序列化器的时候
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .print();


        env.execute();
    }

}
