package com.github.chapter11;

import com.github.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class CumulateWindowExampleCopy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> streamSource = env
                .fromElements(
                        new Event("hwj", "baidu.com", 1000L),
                        new Event("hwj", "bing.com", 3000L),
                        new Event("hbh", "other.com", 3000L),
                        new Event("hwj", "google.com", 6000L),
                        new Event("hwj", "sougou.com", 9999L),
                        new Event("hwj", "haha.com", 10000L),
                        new Event("hwj", "xixi.com", 10001L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                })
                );


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table table1 = tableEnv.fromDataStream(
                streamSource,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
        );

        tableEnv.createTemporaryView("tmpTable", table1);

        Table result = tableEnv.sqlQuery(
                "select user, window_end as endT, count(url) as cnt " +
                        "from table(cumulate(table tmpTable, descriptor(ts), interval '5' second, interval '10' second))" +
                        "group by user, window_start, window_end"
        );
        // 因为结果是没有动态更新的，根据窗口分组的。所以可直接用toDataStream
        tableEnv.toDataStream(result).print();
        env.execute();
    }
}
