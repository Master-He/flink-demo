package com.github.chapter09.section01valuestate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class MyFlatMapFunction extends RichFlatMapFunction<Long, String> {
    // 声明状态
    private transient ValueState<Long> state;

    @Override
    public void open(Configuration config) throws IOException {
        // 在 open 生命周期方法中获取状态
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
            "my state", // 状态名称
            Types.LONG // 状态类型
        );
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Long input, Collector<String> out) throws Exception {
        // 访问状态
        Long currentState = state.value();
        if (currentState == null) {
            currentState = 0L;
        }
        currentState += 1L; // 状态数值加 1
        // 更新状态
        state.update(currentState);
        if (currentState >= 10L) {
            out.collect("state: "+currentState);
            state.clear(); // 清空状态
            state.update(0L);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> longDataStreamSource = env.addSource(new LongValueSource());
        SingleOutputStreamOperator<String> resultStream = longDataStreamSource.
            keyBy((KeySelector<Long, Long>) value -> value % 10L)
            .flatMap(new MyFlatMapFunction());
        resultStream.print();
        env.execute();
    }

    private static class LongValueSource implements SourceFunction<Long> {
        public boolean isRunning = true;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long l = 0L;
            while (isRunning) {
                l++;
                ctx.collect(l);
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}