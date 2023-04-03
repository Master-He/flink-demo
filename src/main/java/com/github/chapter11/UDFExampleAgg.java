package com.github.chapter11;

import com.github.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.e;

public class UDFExampleAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> streamSource = env
                .fromElements(
                        new Event("hwj", "baidu.com", 1000L),
                        new Event("hwj", "bing.com", 3000L),
                        new Event("hbh", "bing.com", 3000L),
                        new Event("hbh", "bing.com", 5000L)
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

        // 注册自定义聚合函数
        tableEnv.createTemporarySystemFunction("my_concat", MyConcat.class);

        // 调用函数计算加权平均值
        Table result = tableEnv.sqlQuery(
                "select user, my_concat(url) from tmpTable group by user"
        );


        tableEnv.toChangelogStream(result).print();
        env.execute();


    }

    public static class ConcatAccumulator {
        public String stringBuffer = "";  // 用 new StringBuilder 会报错，我猜是因为StringBuilder不是flink支持的类型，不确定。。
    }

    // 自定义聚合函数，输出为长整型的平均值，累加器类型为 WeightedAvgAccumulator
    public static class MyConcat extends AggregateFunction<String, ConcatAccumulator> {
        @Override
        public ConcatAccumulator createAccumulator() {
            return new ConcatAccumulator(); // 创建累加器
        }

        @Override
        public String getValue(ConcatAccumulator acc) {
            if (acc.stringBuffer == null) {
                return null;
            } else {
                return acc.stringBuffer;
            }
        }

        // 累加计算方法，每来一行数据都会调用
        public void accumulate(ConcatAccumulator acc, String value) {
            if (!acc.stringBuffer.isEmpty()) {
                acc.stringBuffer += ", ";
            }

            if (value != null) {
                acc.stringBuffer += value;
            }
        }
    }


}
