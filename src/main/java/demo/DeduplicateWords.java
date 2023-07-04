package demo;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class DeduplicateWords {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //windows: nc -lp 9999
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);

        socketDS.flatMap((FlatMapFunction<String, String>) (text, collector) -> Arrays.stream(text.split(" ")).forEach(collector::collect))
                .returns(TypeInformation.of(String.class))
                .filter(StringUtils::isNotBlank)
                .keyBy(word -> word)
                .flatMap(new RichFlatMapFunction<String, String>() {
                    //不同key的状态是隔离的
                    ValueState<Boolean> duplicateIndicator;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("duplicateKey", Types.BOOLEAN);
                        duplicateIndicator = getRuntimeContext().getState(desc);
                    }

                    @Override
                    public void flatMap(String word, Collector<String> collector) throws Exception {
                        if (duplicateIndicator.value() == null) {
                            collector.collect(word);
                            duplicateIndicator.update(true);
                        }
                    }

                })
                .print();

        env.execute();

    }

}
