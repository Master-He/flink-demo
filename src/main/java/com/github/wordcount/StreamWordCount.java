package com.github.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 运行前需要在机器192.168.0.111上运行nc -lk 7777
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取文本流, 在192.168.0.111上运行nc -lk 7777命令
        DataStream<String> lineDSS = env.socketTextStream("192.168.0.111", 7777).name("nc");
        // 3. 转换数据格式
        DataStream<Tuple2<String, Long>> wordAndOne = lineDSS
            .flatMap((String line, Collector<String> words) -> {
                Arrays.stream(line.split(" ")).forEach(words::collect);
            })
            .returns(Types.STRING)
            .map(word -> Tuple2.of(word, 1L))
            .returns(Types.TUPLE(Types.STRING, Types.LONG)).name("transform");
        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);
        // 5. 求和
        DataStream<Tuple2<String, Long>> result = wordAndOneKS.sum(1).name("sum");
        // 6. 打印
        result.print("sum result ");
        // 7. 执行
        env.execute();
    }
}