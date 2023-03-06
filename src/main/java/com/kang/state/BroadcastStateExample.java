package com.kang.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author Akang
 * @create 2023-03-06 10:10
 */
public class BroadcastStateExample {
    private static MapStateDescriptor<String, Tuple3<String, String, Integer>> pattern =
            new MapStateDescriptor<>(
                    "pattern"
                    , Types.STRING
                    , Types.TUPLE(Types.STRING,Types.STRING,Types.INT));

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;

        //1. Event Stream
        DataStreamSource<Tuple2<String, String>> eventStream =
                env.fromElements(Tuple2.of("userID_1", "userName_1"), Tuple2.of("userID_2", "userName_2"));

        //2. Broadcast Stream
        DataStreamSource<Tuple3<String, String, Integer>> patternStream =
                env.fromElements(Tuple3.of("userID_1", "男", 26), Tuple3.of("userID_2", "女", 30));

        BroadcastStream<Tuple3<String, String, Integer>> broadcast = patternStream.broadcast(pattern);

        //connect broadcast
        SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> process = eventStream.keyBy(data -> data.f0)
                .connect(broadcast)
                .process(new MyKeyedBroadcastProcessFunction());
        process.print() ;

        env.execute() ;
    }

    private static class MyKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<
            String
            ,Tuple2<String,String>
            ,Tuple3<String,String,Integer>
            , Tuple4<String,String,String,Integer>>{

        private ValueState<Tuple2<String,String>> eventState ;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<String, String>> stateProperties =
                    new ValueStateDescriptor<>("value-state", Types.TUPLE(Types.STRING, Types.STRING));
            eventState = getRuntimeContext().getState(stateProperties);
        }

        @Override
        public void processBroadcastElement(Tuple3<String, String, Integer> value, Context ctx
                , Collector<Tuple4<String, String, String, Integer>> out) throws Exception {

            BroadcastState<String, Tuple3<String, String, Integer>> broadcastState = ctx.getBroadcastState(pattern);
            broadcastState.put(value.f0,value);
//            System.out.println(broadcastState);

        }

        @Override
        public void processElement(Tuple2<String, String> value
                , ReadOnlyContext ctx, Collector<Tuple4<String, String, String, Integer>> out) throws Exception {

            Tuple3<String, String, Integer> patternStream = ctx.getBroadcastState(pattern).get(value.f0);

            if (patternStream == null){
                eventState.update(value);
                ctx.timerService().registerEventTimeTimer(System.currentTimeMillis()+1);
                System.out.println(value.f0 + " 处理前时间： " + new Timestamp(System.currentTimeMillis()));
            }else{
                out.collect(Tuple4.of(value.f0,value.f1,patternStream.f1,patternStream.f2));
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<String, String, String, Integer>> out)
                throws Exception {
            Tuple2<String, String> value = eventState.value();
            Tuple3<String, String, Integer> patternStream = ctx.getBroadcastState(pattern).get(value.f0);

            out.collect(patternStream == null ? Tuple4.of(value.f0,value.f1,"",0):Tuple4.of(value.f0,value.f1
            ,patternStream.f1,patternStream.f2));
            System.out.println(value.f0 + " 处理后时间" + new Timestamp(timestamp));
        }
    }
}
