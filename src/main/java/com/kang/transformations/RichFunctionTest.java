package com.kang.transformations;

import com.kang.pojo.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Akang
 * @create 2022-10-27 16:49
 */
public class RichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream = env.fromElements(
                new Event("tom", "./cart", 100L),
                new Event("jack", "./mart", 200L));
        env.setParallelism(2) ;
        stream.map(new RichMapFunction<Event, Long>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("索引为" + getRuntimeContext().getIndexOfThisSubtask()+ "的任务开始运行");
            }

            @Override
            public Long map(Event event) throws Exception {
                return event.timestamp;
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("索引为" + getRuntimeContext().getIndexOfThisSubtask() +"的任务结束");
            }
        }).print() ;
        env.execute() ;
    }

}
