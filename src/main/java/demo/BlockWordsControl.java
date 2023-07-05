package demo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class BlockWordsControl {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<String, String> blockWordsDS = env.socketTextStream("localhost", 9999)
                .flatMap((String text, Collector<String> out) -> Arrays.stream(text.split(" ")).forEach(out::collect))
                .returns(Types.STRING)
                .keyBy(word -> word);
        KeyedStream<String, String> wordsDS = env.addSource(new SourceFunction<String>() {

                    public final String[] words = new String[]{"hello", "world", "java", "flink"};

                    private volatile boolean isRunning = true;

                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        while (isRunning) {
                            int index = new Random().nextInt(4);
                            ctx.collect(words[index]);
                            TimeUnit.SECONDS.sleep(2);
                        }
                    }

                    @Override
                    public void cancel() {
                        isRunning = false;
                    }
                })
                .keyBy(word -> word);

        wordsDS.connect(blockWordsDS)
                .flatMap(new RichCoFlatMapFunction<String, String, String>() {

                    ValueState<Boolean> blocked;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("wordsBeenBlocked", Types.BOOLEAN);
                        blocked = getRuntimeContext().getState(desc);
                    }

                    @Override
                    public void flatMap1(String word, Collector<String> out) throws Exception {
                        if (blocked.value() == null) {
                            out.collect(word);
                        }
                    }

                    @Override
                    public void flatMap2(String blockWord, Collector<String> out) throws Exception {
                        blocked.update(true);
                    }
                })
                .print();

        env.execute();
    }

}
