package com.kang.sources;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Akang
 * @create 2023-03-03 18:31
 */
public class MySqlSource extends RichSourceFunction<Map<String,Tuple4<String,String,String,Integer>>> {
    private Boolean flag = true ;
    private Connection conn = null ;
    private PreparedStatement ps  = null ;
    private ResultSet rs = null ;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata/","root","root") ;
        String sql = "select `userId `, `userName`, `userSex`,`userAge` from user_info " ;
        ps = conn.prepareStatement(sql) ;
    }

    @Override
    public void run(SourceContext<Map<String, Tuple4<String, String, String, Integer>>> ctx) throws Exception {
        while (flag){
            Map<String, Tuple4<String,String,String,Integer>> result = new HashMap<>() ;
            rs = ps.executeQuery() ;
            while (rs.next()){
                String userId = rs.getString("userId");
                String userName = rs.getString("userName");
                String userSex = rs.getString("userSex");
                int userAge = rs.getInt("userAge");
                result.put(userId,Tuple4.of(userId,userName,userSex,userAge));
            }
            ctx.collect(result);
        }
        Thread.sleep(5000L);
    }

    @Override
    public void cancel() {
        flag = false ;
    }

    @Override
    public void close() throws Exception {
        if (conn !=null) conn.close();
        if (ps != null) ps.close();
        if (rs!=null) rs.close();
    }
}
