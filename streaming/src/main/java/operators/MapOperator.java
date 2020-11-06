package operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class MapOperator {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple1<String>> dataStream  =  env.fromElements(Tuple1.of("This is a test"), Tuple1.of("another test"));

        dataStream.map (new MapFunction<Tuple1<String>, String>() {

            @Override
            public String map(Tuple1<String> tuple1) throws Exception {
                return Arrays.toString( tuple1.f0.split("\\s"));
            }
        }).print();

        env.execute();
    }
}
