package com.kang.wordconut;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author Akang
 * @create 2022-10-16 21:04
 */
public class BoundStreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");
        // 3. 转换操作
        SingleOutputStreamOperator<Tuple2<String, Long>> LineDataOutputStream = lineDataStreamSource.flatMap(
                (String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        //out.collect(new Tuple2(word, 1L));
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 数据分组
        KeyedStream<Tuple2<String, Long>, String> LineStreamGroup = LineDataOutputStream.keyBy(
                data -> data.f0
        );

        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = LineStreamGroup.sum(1);

        // 6. 打印
        sum.print();
        // 7. 开始执行任务
        env.execute();
    }
}
