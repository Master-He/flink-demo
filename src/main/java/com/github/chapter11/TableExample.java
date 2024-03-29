import com.github.chapter05.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableExample {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据源
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );
        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将数据流转换成表 (这个时候的表没有注册进表环境， 还是java对象)
        Table eventTable = tableEnv.fromDataStream(eventStream);
        // 用执行 SQL 的方式提取数据
        // 由于流中的数据本身就是定义好的 POJO 类型 Event，所以我们将流转换成表之后，
        // 每一行数据就对应着一个 Event，而表中的列名就对应着 Event 中的属性。
        Table visitTable = tableEnv.sqlQuery("select url, user from " + eventTable);
        // 将表转换成数据流，打印输出
        tableEnv.toDataStream(visitTable).print();
        // 执行程序
        env.execute();
    }
}