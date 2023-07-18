package demo;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;

public class Fibonacci {
    public static void main(String[] args) throws Exception {
        //计算前n个数
        final int n = 10;

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Tuple2<Long, Long>> input = env.fromElements(
                Tuple2.of(0L, 1L)
        );

        IterativeDataSet<Tuple2<Long, Long>> iteration = input.iterate(n);

        MapOperator<Tuple2<Long, Long>, Tuple2<Long, Long>> nextFibonacci = iteration
                .map(value -> Tuple2.of(value.f1, value.f0 + value.f1))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));

        DataSet<Tuple2<Long, Long>> completedIteration = iteration
                .closeWith(nextFibonacci);

        completedIteration
                .map(tuple -> tuple.f0)
                .print();


//        env.execute();
    }
}
