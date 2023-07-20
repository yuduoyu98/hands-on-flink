package demo.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.PrintStream;

import static common.utils.TimeUtils.formatTs;

public class WatermarkPrintSink<T> extends RichSinkFunction<T> {

    private int subTaskId;

    private final Boolean stdErr;

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
        StringBuilder sb = new StringBuilder();
        sb.append("WatermarkPrintSink subtask ").append(String.format("%02d", subTaskId)).append("> ");
        sb.append("当前线程ID：").append(Thread.currentThread().getId()).append(",");
        sb.append("事件时间：[").append(context.timestamp()).append("|").append(formatTs(context.timestamp())).append("],");
        sb.append("水位线：[").append(context.currentWatermark()).append("|").append(formatTs(context.currentWatermark())).append("],");
        sb.append("事件：").append(value);
        stream.println(sb);
    }
}
