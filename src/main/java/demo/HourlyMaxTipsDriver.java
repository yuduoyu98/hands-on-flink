/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package demo;

import demo.bean.taxi.datatypes.TaxiFare;
import demo.bean.taxi.sources.TaxiFareGenerator;
import demo.functions.SumTipsPerHourKeyedProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyMaxTipsDriver {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    public static final OutputTag<TaxiFare> lateFares = new OutputTag<TaxiFare>("lateFares") {
    };

    /** Creates a job using the source and sink provided. */
    public HourlyMaxTipsDriver(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyMaxTipsDriver job =
                new HourlyMaxTipsDriver(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Create and execute the hourly tips pipeline.
     */
    public void execute() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiFare> fares = env
                .addSource(source)
                .assignTimestampsAndWatermarks(
                        // taxi fares are in order
                        WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (fare, t) -> fare.getEventTimeMillis()));

        DataStream<Tuple3<Long, Long, Float>> hourlyMax = solution1(fares);
//        DataStream<Tuple3<Long, Long, Float>> hourlyMax = solution2(fares);
//        DataStream<Tuple3<Long, Long, Float>> hourlyMax = solution3(fares);

        hourlyMax.addSink(sink);

        env.execute("Hourly Tips");
    }

    /**
     * 1.按司机聚合出每小时的总小费
     * 2.求最大值
     */
    @SuppressWarnings("unused")
    private static DataStream<Tuple3<Long, Long, Float>> solution1(DataStream<TaxiFare> fares) {
        // compute tips per hour for each driver

        return fares.keyBy((TaxiFare fare) -> fare.driverId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow>() {
                    @Override
                    public void process(
                            Long key,
                            Context context,
                            Iterable<TaxiFare> fares1,
                            Collector<Tuple3<Long, Long, Float>> out) {

                        float sumOfTips = 0F;
                        for (TaxiFare f : fares1) {
                            sumOfTips += f.tip;
                        }
                        out.collect(Tuple3.of(context.window().getEnd(), key, sumOfTips));
                    }
                })
                //window后接windowAll可以成立：1.事件时间 2.第一个窗口的output的事件时间均为窗口结束时间
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                //所有key的窗口计算就绪才会触发windowAll的计算?(gpt) 源码怎么实现的？
                .maxBy(2);
    }

    /**
     * 直接开1h全局滚动窗口去计算
     * 对比第一种方法
     *      触发时机：某个司机TaxiFare的事件时间满足触发条件 而不会等到所有司机(key)都有一条满足触发条件的TaxiFare? 计算更不准？
     */
    @SuppressWarnings("unused")
    private static DataStream<Tuple3<Long, Long, Float>> solution2(DataStream<TaxiFare> fares) {
        return fares
                .map(fare -> Tuple2.of(fare.driverId, fare.tip))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Long, Float>>() {
                }))
                //假如司机A比B的TaxiFare延迟低很多 提前触发窗口？
                //这样process方法存储的数据量很大？
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1L)))
                //为什么要用process? 要通过context拿到对应触发窗口结束时间
                .process(new ProcessAllWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, TimeWindow>() {
                    @Override
                    public void process(
                            Context context,
                            Iterable<Tuple2<Long, Float>> fares1,
                            Collector<Tuple3<Long, Long, Float>> out) {
                        HashMap<Long, Float> map = new HashMap<>();
                        long endTs = context.window().getEnd();
                        for (Tuple2<Long, Float> fare : fares1) {
                            Long driverId = fare.f0;
                            Float tip = fare.f1;
                            if (!map.containsKey(driverId)) {
                                map.put(driverId, 0f);
                            }
                            float total = map.get(driverId) + tip;
                            map.put(driverId, total);
                        }
                        Float maxTip = 0f;
                        Long maxDriverId = null;
                        for (Long driverId : map.keySet()) {
                            Float driverTip = map.get(driverId);
                            if (driverTip > maxTip) {
                                maxDriverId = driverId;
                                maxTip = driverTip;
                            }
                        }
                        out.collect(Tuple3.of(endTs, maxDriverId, maxTip));
                    }
                });
    }

    /**
     * 使用KeyedProcessFunction实现滚动窗口以及窗口计算(增量)
     */
    @SuppressWarnings("unused")
    private static DataStream<Tuple3<Long, Long, Float>> solution3(DataStream<TaxiFare> fares) {
        SingleOutputStreamOperator<Tuple3<Long, Long, Float>> hourlySum = fares
                .keyBy(fare -> fare.driverId)
                //KeyedProcessFunction实现一个1h滚动窗口计算
                .process(new SumTipsPerHourKeyedProcessFunction(Time.hours(1)));
        hourlySum.getSideOutput(lateFares).print();
        return hourlySum
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .max(2);
    }

}
