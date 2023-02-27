package com.kang.sources;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author Akang
 * @create 2022-10-26 23:38
 */
public class SourceKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop101:9092");
//        props.setProperty("group.id", "consumer-group");
//        props.setProperty("key.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        props.setProperty("value.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>(
                "clicks", new SimpleStringSchema(), props
        ));

        stream.print("kafka");
        env.execute();
    }
    public static class  CustomWatermarkStrategy implements WatermarkStrategy<String>{
        @Override
        public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return null;
        }

        @Override
        public TimestampAssigner<String> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return null;
        }
    }
}
