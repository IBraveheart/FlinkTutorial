package com.kang.wordconut;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Akang
 * @create 2022-10-17 14:34
 * Dataset API
 * Notes: 从flink 1.12 版本开始进行了流批统一API，统一用DataStream API 编写流处理和批处理程序，只是在提交的时候会有区别。
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 加载数据
        DataSource<String> lineSource = env.readTextFile("input/words.txt");

        // 3. 数据转换
        FlatMapOperator<String, Tuple2<String, Long>> wordWithOne = lineSource.flatMap(
                (String line, Collector<Tuple2<String, Long>> out) -> {
                String[] words = line.split(" ");
                for(String word:words){
                    out.collect(Tuple2.of(word,1L));
                }
            }).returns(Types.TUPLE(Types.STRING,Types.LONG)) ;   // 因为泛型 lambda 表达式会存在类型擦除

        // 4. 分组
        UnsortedGrouping<Tuple2<String, Long>> groupLine = wordWithOne.groupBy(0);

        // 5. 求和
        AggregateOperator<Tuple2<String, Long>> sum = groupLine.sum(1);

        // 6. 打印
        sum.print();
    }
}
