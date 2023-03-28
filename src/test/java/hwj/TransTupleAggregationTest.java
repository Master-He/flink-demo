package hwj;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransTupleAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple3<String, Integer, String>> stream = env.fromElements(
            Tuple3.of("a", 8, "A"),
            Tuple3.of("a", 3, "B"),
            Tuple3.of("b", 9, "C"),
            Tuple3.of("b", 4, "D")
        );
        stream.keyBy(r -> r.f0).min("f1").print();
//        stream.keyBy(r -> r.f0).minBy("f1").print();
        env.execute();
    }
}