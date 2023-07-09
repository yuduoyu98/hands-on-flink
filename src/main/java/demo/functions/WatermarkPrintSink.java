package demo.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.PrintStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class WatermarkPrintSink<T> extends RichSinkFunction<T> {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private int subTaskId;

    private final Boolean stdErr;

    private static String formatTs(Long ts) {
        Instant instant = Instant.ofEpochMilli(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        return localDateTime.format(formatter);
    }

    public WatermarkPrintSink(boolean stdErr) {
        this.stdErr = stdErr;
    }

    public WatermarkPrintSink() {
        this(false);
    }

    @Override
    public void open(Configuration parameters) {
        subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void invoke(T value, Context context) {
        PrintStream stream = this.stdErr ? System.err : System.out;
        synchronized (stream) {
            stream.println("---------------------------------- subtask " + subTaskId + " ----------------------------------");
            stream.println(subTaskId + "> event:     " + value);
            stream.println(subTaskId + "> event_ts:  " + formatTs(context.timestamp()));
            stream.println(subTaskId + "> watermark: " + formatTs(context.currentWatermark()));
        }
    }
}
