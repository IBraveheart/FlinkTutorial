package com.kang.sources;

import com.kang.pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;


/**
 * @author Akang
 * @create 2022-10-26 23:46
 */
public class MySource implements SourceFunction<Event> {
    private Boolean running = true;

    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();
        String[] users = {"kang", "lin"};
        String[] urls = {"./name", "./cart", "./fav"};
        while (running) {
            sourceContext.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            Thread.sleep(1000);
        }
    }

    public void cancel() {
        running = false;
    }
}
