package com.kang.pojo;


import java.io.Serializable;
import java.sql.Timestamp;

/**
 * @author Akang
 * @create 2022-10-26 17:14
 */
public class Event implements Serializable {
   // private static final long serialVersionUID = -6849794470754667721L;

    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user,String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" + "user=" + this.user +
                ", url=" + this.url +
                ", timestamp=" + new Timestamp(this.timestamp)  +
                "}" ;
    }
}
