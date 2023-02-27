package com.kang.sources;

import com.kang.pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author Akang
 * @create 2023-02-22 21:12
 */
public class ClickSource implements SourceFunction<Event> {
    private boolean flag = true ;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();
        String users[] = {"Mary", "Alice" , "Bob", "Cary"} ;
        String urls[] = {"./home","./cart","./fav","./prod?id=1","./prod?id=2"} ;

        while (flag){
            Event event = new Event(users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis());
            ctx.collect(event);


            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.flag = false ;
    }
}
