package com.kang.wordconut;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.Types;


/**
 * @author Akang
 * @create 2022-10-17 15:07
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 加载数据
        DataStreamSource<String> lineDSS = env.socketTextStream("hadoop101", 9999);
        // 数据转换
        SingleOutputStreamOperator<Tuple2<String, Long>> wordWithOne = lineDSS.flatMap(
                (String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 分组
        KeyedStream<Tuple2<String, Long>, String> groupLine = wordWithOne.keyBy(data -> data.f0);
        // 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = groupLine.sum(1);
        // 打印
        sum.print();
        // 执行
        env.execute();
    }
}
