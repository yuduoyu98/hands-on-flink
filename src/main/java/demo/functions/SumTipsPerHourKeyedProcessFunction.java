package demo.functions;


import demo.bean.taxi.datatypes.TaxiFare;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;

/**
 * 计算一小时内每个司机的小费总和
 * 使用KeyedProcessFunction实现滚动窗口以及窗口计算
 */
public class SumTipsPerHourKeyedProcessFunction extends KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {

    private transient MapState<Long, Float> sumPerHour;

    private static final OutputTag<TaxiFare> lateFares = new OutputTag<TaxiFare>("lateFares") {
    };

    private final long duration;

    public SumTipsPerHourKeyedProcessFunction(Time duration) {
        TimeUnit unit = duration.getUnit();
        this.duration = unit.toMillis(duration.getSize());
    }

    /**
     * 由于可能出现乱序情况，所以同一时刻可能有多个窗口存在
     * 需要一个MapState来记录每个小时窗口该司机(key)的小费总和
     */
    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<Long, Float> desc = new MapStateDescriptor<>("sumPerHour", Types.LONG, Types.FLOAT);
        sumPerHour = getRuntimeContext().getMapState(desc);
    }

    @Override
    public void processElement(
            TaxiFare fare,
            Context ctx,
            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        //ctx.timestamp即为当前事件的事件时间 =fare.getEventTimeMillis()
        Long eventTs = ctx.timestamp();
        //timeService -> 注册timer + watermark
        TimerService timerService = ctx.timerService();
        if (eventTs <= timerService.currentWatermark()) {
            //迟到事件处理：侧输出流
            ctx.output(lateFares, fare);
        } else {
            //计算窗口结束时间，注册窗口Timer
            long windowEndTs = (eventTs / duration + 1) * duration;
            timerService.registerEventTimeTimer(windowEndTs);

            //计算
            if (!sumPerHour.contains(windowEndTs)) {
                sumPerHour.put(windowEndTs, fare.tip);
            } else {
                float newSum = sumPerHour.get(windowEndTs) + fare.tip;
                sumPerHour.put(windowEndTs, newSum);
            }

        }


    }

    @Override
    public void onTimer(
            long windowEndTs,
            OnTimerContext ctx,
            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        //计算每个司机这一个小时的小费总数
        Float sum = sumPerHour.get(windowEndTs);
        Long driverId = ctx.getCurrentKey();
        out.collect(Tuple3.of(windowEndTs, driverId, sum));
        //清除无效状态
        sumPerHour.remove(windowEndTs);
    }
}