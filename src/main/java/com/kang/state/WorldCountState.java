package com.kang.state;

import com.kang.pojo.Event;
import com.kang.sources.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Akang
 * @create 2023-02-28 21:46
 */
public class WorldCountState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;

        //获取数据源
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        eventStream.print("Event") ;

        eventStream.keyBy(data->data.url).flatMap(new MyFlatMap())
                .print("Result") ;


        env.execute() ;
    }

    private static class MyFlatMap extends RichFlatMapFunction<Event,Tuple2<String,Long>>{
        MapState<String,Long> mapState ;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("mymap-state"
                     ,String.class,Long.class)) ;
        }

        @Override
        public void flatMap(Event value, Collector<Tuple2<String, Long>> out) throws Exception {
            if (mapState.contains(value.url)) {
                mapState.put(value.url,mapState.get(value.url) + 1L);
            }else {
                mapState.put(value.url,1L);
            }

            out.collect(Tuple2.of(value.url,mapState.get(value.url)));
        }
    }
}
