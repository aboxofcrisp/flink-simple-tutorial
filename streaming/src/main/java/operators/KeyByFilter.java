package operators;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

public class KeyByFilter {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(2);
        DataStream<Tuple2<String,String>> dataStream  =
                env.fromElements(
                        new Tuple2<String,String>("name","aboxofcrisp@gmail.com"),
                        new Tuple2<String,String>("name","unkown"),
                        new Tuple2<String,String>("city","unkown"),
                        new Tuple2<String,String>("city","unkown"));

        dataStream.keyBy(value -> value._1 ).print() ;


        env.execute();
    }
}
