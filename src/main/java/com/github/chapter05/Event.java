package com.github.chapter05;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Timestamp;
import java.util.ArrayList;


public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
            "user='" + user + '\'' +
            ", url='" + url + '\'' +
            ", timestamp=" + new Timestamp(timestamp) +
            '}';
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary","./home",1000L));
        clicks.add(new Event("Bob","./cart",2000L));
        DataStream<Event> stream = env.fromCollection(clicks);
        stream.print();
        env.execute();
    }
}