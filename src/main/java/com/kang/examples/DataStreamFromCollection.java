package com.kang.examples;

import com.kang.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author Akang
 * @create 2022-10-26 17:19
 */
public class DataStreamFromCollection {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment() ;
        env.setParallelism(1) ;
        // 2. 获取数据
        ArrayList<Event> datas = new ArrayList<>();
        datas.add(new Event("Mary","./home",100L)) ;
        datas.add(new Event("Bob","./carg",200L));
        DataStreamSource<Event> stream = env.fromCollection(datas);
        // 3. 打印
        stream.print() ;
        // 4. 执行
        env.execute() ;
    }
}
