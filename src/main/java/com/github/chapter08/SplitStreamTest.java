package com.github.chapter08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SplitStreamTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // todo continue
    }
}
