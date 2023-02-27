package com.kang.examples;


import com.kang.pojo.Event;
import com.kang.pojo.UrlViewCount;
import com.kang.sources.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author Akang
 * @create 2023-02-22 21:00
 */
public class KeyedProcessTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;
        // 获得数据源
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        eventStream.print() ;

        SingleOutputStreamOperator<UrlViewCount> urlCountStream = eventStream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());

        SingleOutputStreamOperator<String> result = urlCountStream.keyBy(data -> data.windowEnd)
                .process(new Top(2));

        result.print("result") ;

        env.execute() ;
    }

    private static class UrlViewCountAgg implements AggregateFunction<Event,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    private static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount,String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<UrlViewCount> out)
                throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect(new UrlViewCount(s,elements.iterator().next(),start,end));
        }

    }

    private static class Top extends KeyedProcessFunction<Long, UrlViewCount, String> {
        private int n  ;
        private ListState<UrlViewCount> urlViewCountListState ;

        public Top(int n) {
            this.n = n ;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从环境中获得状态句柄
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-view-count", Types.POJO(UrlViewCount.class))) ;
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            urlViewCountListState.add(value);

            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> urlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount u : urlViewCountListState.get()){
                urlViewCountArrayList.add(u) ;
            }
            // 清空状态释放资源
            urlViewCountListState.clear();


            urlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue() ;
                }
            });

            // 取前N名作为输出
            StringBuilder result = new StringBuilder() ;
            result.append("======================================\n") ;
            result.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
            for (int i = 0; i < this.n; i++) {
                UrlViewCount urlViewCount = urlViewCountArrayList.get(i);
                String info = "No." + (i +1 ) + " " + "url:" + urlViewCount.url + " "
                        + "浏览量:" + + urlViewCount.count + "\n" ;
                result.append(info) ;
            }
            result.append("======================================\n") ;

            out.collect(result.toString());

        }
    }
}

