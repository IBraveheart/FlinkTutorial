package com.kang.sources;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @author Akang
 * @create 2022-10-27 11:24
 */
public class MySourceParallel {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 获取数据
        DataStream<Integer> stream = env.addSource(new SourceParallel()).setParallelism(2);
        stream.print().setParallelism(1);
        env.execute() ;
    }
    private static class SourceParallel implements ParallelSourceFunction<Integer>{
        private Boolean running = true ;
        private Random random = new Random() ;
        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while (running){
                sourceContext.collect(random.nextInt());
            }
            Thread.sleep(1000);
        }

        @Override
        public void cancel() {
            running = false ;
        }
    }
}
