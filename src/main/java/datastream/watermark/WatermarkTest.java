package datastream.watermark;

import common.test.KeywordEvent;
import common.test.KeywordEventBasicFunctions;
import demo.functions.WatermarkPrintSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * <h3>测试watermark的生成情况</h3>
 * <strong>说明<strong/>：
 * <ol>
 *  <li><tt>disable chaining</tt>方便在web ui里直观看到sink每个<tt>subtask</tt>的<tt>watermark</tt>
 *  <li>使用{@link WatermarkPrintSink WatermarkPrintSink}打印水位线/事件时间等相关信息
 *  <li>在{@link KeywordEventBasicFunctions.PunctuatedWatermarkGenerator#onEvent PunctuatedWatermarkGenerator#onEvnet}方法中打印<tt>watermark</tt>生成相关信息
 * </ol>
 * <p> <strong>测试场景1</strong>：使用本地默认并行度（16）查看watermark生成情况
 * <p> <strong>现象与结论</strong>：
 * <pre>
 *   <tt>socketTextStream</tt>为单并行度<tt>Source</tt>，采用<tt>rebalance</tt>策略（<tt>round-robin</tt>）
 *   轮询发往下游task（并行度16），因此发送16条（并行度）后的第17条才会进入
 *   处理第1条的<tt>subtask</tt>中（<tt>watermark</tt>的生成策略为非周期的/逐条生成），因此前
 *   16条{@link WatermarkPrintSink WatermarkPrintSink}都无法打印出<tt>watermark</tt>，因为<tt>subtask</tt>切换了，第17
 *   条才能打印出。
 * </pre>
 * <p> <strong>测试场景2</strong>：将<tt>sink</tt>并行度改为1
 * <p> <strong>现象与结论</strong>：
 * <pre>
 *     还是需要发送n条后的第n+1条（n为<tt>sink</tt>上游算子并行度），才能看到<tt>watermark</tt>更新
 *     因为涉及到多入度情况下<tt>watermark</tt>会等待所有入度的<tt>watermark</tt>到达后拿最大的当作
 * </pre>
 * <p> <strong>源码分析</strong>：
 * <ul>
 *    <li>{@link org.apache.flink.streaming.api.datastream.DataStream#assignTimestampsAndWatermarks DataStream#assignTimestampsAndWatermarks}后4并行度汇总为1并行度的<tt>Sink</tt>
 *    <li>{@link org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput#processElement StreamTaskNetworkInput#processElement}处理每个<tt>StreamRecord</tt>
 *    <li>{@link org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve#inputWatermark StatusWatermarkValve#inputWatermark}处理收到的来自每个<tt>channel</tt>的<tt>watermark</tt>
 *    <li>{@link org.apache.flink.streaming.runtime.watermarkstatus.HeapPriorityQueue watermarkstatus.HeapPriorityQueue}(非{@link org.apache.flink.runtime.state.heap.HeapPriorityQueue heap.HeapPriorityQueue})对各个<tt>channel</tt>的<tt>watermark</tt>进行排序（最小堆排序）
 *    <li>每当从某个<tt>channel</tt>收到新的<tt>watermark</tt>时，会先更新信道状态{@link org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve#channelStatuses channelStatuses}，然后调整堆的排序{@link org.apache.flink.streaming.runtime.watermarkstatus.HeapPriorityQueue#adjustModifiedElement adjustModifiedElement}
 *    <li>{@link org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve#findAndOutputNewMinWatermarkAcrossAlignedChannels StatusWatermarkValve#findAndOutputNewMinWatermarkAcrossAlignedChannels}尝试<tt>emit watermark</tt>（堆最小<tt>watermark</tt>大于上次<tt>emit</tt>的<tt>watermark</tt>）
 * </ul>
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("rest.port", "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);

        env
                .socketTextStream("localhost", 9999)
                .map(new KeywordEventBasicFunctions.SocketText2KeywordEvent())
//                .setParallelism(1)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<KeywordEvent>forGenerator(new KeywordEventBasicFunctions.PunctuatedWatermarkGeneratorSupplier<>(Duration.ofSeconds(2L)))
                                .withTimestampAssigner(new KeywordEventBasicFunctions.keywordEventTsExtractor())
//                                .withIdleness()
                )
                .disableChaining()
                .addSink(new WatermarkPrintSink<>())
                .setParallelism(1);

        env.execute("Watermark测试");
    }
}
