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
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class UDFExampleTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> streamSource = env
                .fromElements(
                        new Event("hwj", "baidu.com", 1000L),
                        new Event("hwj,hbh", "bing.com", 3000L),
                        new Event("hbh", "bing.com", 3000L)
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

        // 注册函数
        tableEnv.createTemporarySystemFunction("split_with_comma", SplitFunction.class);

        // lateral 我的理解是自己关联自己
        Table result = tableEnv.sqlQuery("select split_user from tmpTable, " +
                "lateral table(split_with_comma(user)) T(split_user)");
        // 因为结果是没有动态更新的，根据窗口分组的。所以可直接用toDataStream
        tableEnv.toDataStream(result).print();
        env.execute();

        /*
        * 输出结果
            6> +I[hwj]
            5> +I[hwj]
            7> +I[hbh]
            8> +I[hbh]
        * */

    }

    @FunctionHint(output = @DataTypeHint("ROW<split_user STRING>"))
    public static class SplitFunction extends TableFunction<Row> {
        public void eval(String str) {
            for (String s : str.split(",")) {
                collect(Row.of(s));
            }
        }
    }


}
