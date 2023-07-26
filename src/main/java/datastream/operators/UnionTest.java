package datastream.operators;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds1 = env.fromElements(1, 3, 5, 7, 9);
        DataStreamSource<Integer> ds2 = env.fromElements(2, 4, 6, 8, 10);
        ds1.union(ds2).print();
        env.execute();
    }
}
