package com.github.chapter11;

import com.github.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/*
* 什么是appendQuery?
* 我的理解是： 所有输出结果都以+I 为前缀 （I 表示insert），所有的结果都是以 INSERT 操作追加到结果表中的
* */
public class AppendQueryExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据源，并分配时间戳、生成水位线
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long
                                            recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将数据流转换成表，并指定时间属性
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
                // 将 timestamp 指定为事件时间，并命名为 ts
        );
        // 为方便在 SQL 中引用，在环境中注册表 EventTable
        tableEnv.createTemporaryView("EventTable", eventTable);
        // 设置 1 小时滚动窗口，执行 SQL 统计查询
//        Table result = tableEnv.sqlQuery(
//                "SELECT " +
//                        "user, " +
//                        "window_end AS endT, " +         // 窗口结束时间
//                        "COUNT(url) AS cnt " +         // 统计 url 访问次数
//                        "FROM TABLE( " +
//                        "TUMBLE( TABLE EventTable, " +         // 1 小时滚动窗口
//                        "DESCRIPTOR(ts), " +
//                        "INTERVAL '1' HOUR)) " +
//                        "GROUP BY user, window_start, window_end "
//        );
        // 不区分大小写
        Table result = tableEnv.sqlQuery(
                "select user, window_end as endT, count(url) as cnt " +
                        "from table( tumble( table EventTable, descriptor(ts), interval '1' hour ) )" +
                        "group by user, window_start, window_end "
        );

        // 因为结果是没有动态更新的，根据窗口分组的。所以可直接用toDataStream
        tableEnv.toDataStream(result).print();
        env.execute();
    }
}