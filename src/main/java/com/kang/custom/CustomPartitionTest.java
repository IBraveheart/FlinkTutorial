package com.kang.custom;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Akang
 * @create 2022-10-27 17:12
 */
public class CustomPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);
        stream.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer integer, int i) {
                return integer % 2 ;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer integer) throws Exception {
                return integer;
            }
        }).print().setParallelism(4) ;
        env.execute() ;
    }
}
