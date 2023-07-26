package datastream.sources.file.test;

import datastream.sources.file.impl.FileSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(new FileSource(), WatermarkStrategy.noWatermarks(), "File Source")
                .print();
        env.execute();
    }
}
