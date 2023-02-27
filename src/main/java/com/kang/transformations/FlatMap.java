package com.kang.transformations;

import com.kang.pojo.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Akang
 * @create 2022-10-27 15:07
 */
public class FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream = env.fromElements(new Event("tom", "./cart", 100L),
                new Event("jack", "./mart", 200L));
        SingleOutputStreamOperator<String> output = stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> collector) throws Exception {
                if (event.user.equals("tom")) {
                    collector.collect(event.user);
                } else if (event.user.equals("jack")) {
                    collector.collect(event.user);
                    collector.collect(event.url);

                }
            }
        });
        output.print() ;
        env.execute() ;
    }
}
