package com.kang.sinks;

import com.kang.pojo.Event;
import com.kang.sources.MySource;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author Akang
 * @create 2022-10-28 15:36
 */
public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        DataStream<Event> stream = env.fromElements(
//                new Event("Mary", "./home", 1000L),
//                new Event("Bob", "./cart", 2000L),
//                new Event("Alice", "./prod?id=100", 3000L),
//                new Event("Alice", "./prod?id=200", 3500L),
//                new Event("Bob", "./prod?id=2", 2500L),
//                new Event("Alice", "./prod?id=300", 3600L),
//                new Event("Bob", "./home", 3000L),
//                new Event("Bob", "./prod?id=1", 2300L),
//                new Event("Bob", "./prod?id=3", 3300L));

        DataStreamSource<Event> stream = env.addSource(new MySource());

        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop101:9092");

        stream.addSink(new FlinkKafkaProducer<Event>("clicks", new SerializationSchema<Event>() {
            @Override
            public byte[] serialize(Event event) {
                byte[] bytes = event.toString().getBytes();
                return bytes;
            }
        }, props));

        env.execute();
    }
}
