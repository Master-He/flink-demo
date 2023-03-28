package com.github.chapter05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RescaleTest {

    private static final Logger LOG = LogManager.getLogger(RescaleTest.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 这里使用了并行数据源的富函数版本
        // 这样可以调用 getRuntimeContext 方法来获取运行时上下文的一些信息
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                for (int i = 1; i <= 80; i++) {
                    // 将奇数发送到索引为 1 的并行子任务
                    // 将偶数发送到索引为 0 的并行子任务
                    if (i % 4 == getRuntimeContext().getIndexOfThisSubtask() % 4) {
                        LOG.warn("子任务索引：" + getRuntimeContext().getIndexOfThisSubtask() + "收集数据：" + i);
                        sourceContext.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {
            }
        })
            .setParallelism(4)
            .rescale()
            .print().setParallelism(8); // 注意flink task manager slot要大于8
        env.execute();
    }
}