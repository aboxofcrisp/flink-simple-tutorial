package operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapOperator {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple1<String>> dataStream  =  env.fromElements(Tuple1.of("This is a test"), Tuple1.of("another test"));

        dataStream.flatMap(new FlatMapFunction<Tuple1<String>, String>() {
            @Override
            public void flatMap(Tuple1<String> tuple1, Collector<String> out)
                    throws Exception {
                for(String word: tuple1.f0.split(" ")){
                    out.collect(word);
                }
            }
        }).print();

        env.execute();
    }
}
