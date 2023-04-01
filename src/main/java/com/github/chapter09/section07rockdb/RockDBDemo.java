package com.github.chapter09.section07rockdb;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * @author hewenji
 * @Date 2023/3/30 18:33
 * 输入： [a, b, c, a, a] 类似这样的随机序列
 * 输出： a: 1
 * 输出： b: 1
 * 输出： c: 1
 * 输出： a: 2
 * 输出： a: 3
 *
 * 状态存在RockDB里面，状态存每个字母的的数量
 */
public class RockDBDemo extends RichMapFunction<String, String> {

    private transient ValueState<Integer> count;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("countState", Types.INT);
        this.count = getRuntimeContext().getState(descriptor);
    }

    @Override
    public String map(String value) throws Exception {
        Integer countState = this.count.value();
        if (countState == null) {
            countState = 0;
        }

        countState += 1;

        count.update(countState);

        return value + ": " + countState;

    }


    private static class MySourceFunction implements SourceFunction<String> {

        public boolean isRunning = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            Random random = new Random();

            ArrayList<String> strings = new ArrayList<>(Arrays.asList("a", "b", "c"));

            while (this.isRunning) {
                ctx.collect(strings.get(random.nextInt(3)));
                Thread.sleep(2 * 1000);
            }
        }

        @Override
        public void cancel() {
            this.isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(10 * 1000);

        DataStream<String> stream = env.addSource(new MySourceFunction());
        stream.keyBy(str -> str)
            .map(new RockDBDemo())
            .print();

        env.execute();

    }

}
