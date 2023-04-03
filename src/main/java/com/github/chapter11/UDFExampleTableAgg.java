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
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class UDFExampleTableAgg {
    public static void main(String[] args) throws Exception {
        // 参考: https://www.bilibili.com/video/BV133411s7Sa?p=153&vd_source=6cd527c3a43bcb0943d3d64a7923b3bc
        // 因为sql api 不支持表聚合函数， 只有tabla api支持，所以这里就不写代码了
    }

}
