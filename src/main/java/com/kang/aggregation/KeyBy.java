package com.kang.aggregation;

import com.kang.pojo.Event;
import com.kang.sources.MySource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Akang
 * @create 2022-10-27 15:15
 */
public class KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream = env.addSource(new MySource());
        SingleOutputStreamOperator<Tuple2<Event, Long>> map = stream.map(new MapFunction<Event, Tuple2<Event, Long>>() {
            @Override
            public Tuple2<Event, Long> map(Event event) throws Exception {
                return Tuple2.of(event, 1L);
            }
        });
        KeyedStream<Tuple2<Event, Long>, String> keyEvent = map.keyBy(new KeySelector<Tuple2<Event, Long>, String>() {
            @Override
            public String getKey(Tuple2<Event, Long> e) throws Exception {
                return e.f0.user;
            }
        });

        keyEvent.sum(1).print() ;
        env.execute() ;
    }
}
