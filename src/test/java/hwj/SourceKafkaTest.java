package hwj;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;

import java.util.Properties;

public class SourceKafkaTest {
    public static void main(String[] args) throws Exception {


        Row row = new Row(3);
        row.setField(0, "num1");
        row.setField(1, "num2");
        row.setField(2, 123);
        System.out.println(row);


    }
}