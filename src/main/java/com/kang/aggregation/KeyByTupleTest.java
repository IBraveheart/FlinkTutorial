package com.kang.aggregation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Akang
 * @create 2022-10-27 15:57
 */
public class KeyByTupleTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4));

//        stream.keyBy(r -> r.f0).sum(1).print();
//        stream.keyBy(r -> r.f0).sum("f1").print();
//        stream.keyBy(r -> r.f0).max(1).print();
//        stream.keyBy(r -> r.f0).max("f1").print();
//        stream.keyBy(r -> r.f0).min(1).print();
//        stream.keyBy(r -> r.f0).min("f1").print();
//        stream.keyBy(r -> r.f0).maxBy(1).print();
//        stream.keyBy(r -> r.f0).maxBy("f1").print();
//        stream.keyBy(r -> r.f0).minBy(1).print();
        SingleOutputStreamOperator<Tuple2<String, Integer>> out =
                stream.keyBy(r -> r.f0).minBy("f1");
        out.print();

        env.execute();
    }
}
