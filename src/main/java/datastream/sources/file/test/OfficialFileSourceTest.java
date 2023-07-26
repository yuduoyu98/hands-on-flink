package datastream.sources.file.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OfficialFileSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("src/main/java/datastream/sources/file/test/data/test.txt"))
                .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "FileSource")
                .print();
        env.execute();
    }
}
