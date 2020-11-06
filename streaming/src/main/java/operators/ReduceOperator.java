package operators;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ReduceOperator {



    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, String, Integer>> dataStreamSource = env.addSource(new SourceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public void run(SourceFunction.SourceContext<Tuple3<String, String, Integer>> ctx) throws Exception {
                ctx.collect(Tuple3.of("Lisi", "Math", 1));
                ctx.collect(Tuple3.of("Lisi", "English", 2));
                ctx.collect(Tuple3.of("Lisi", "Chinese", 3));

                ctx.collect(Tuple3.of("Zhangsan", "Math", 4));
                ctx.collect(Tuple3.of("Zhangsan", "English", 5));
                ctx.collect(Tuple3.of("Zhangsan", "Chinese", 6));
            }

            @Override
            public void cancel() {

            }
        }, "mySource");


        /**
         * 代码段2
         */
//        src1.keyBy(0).reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
//            @Override
//            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) throws Exception {
//                return Tuple3.of(value1.f0, "总分:", value1.f2 + value2.f2);
//            }
//        }).print();
//        1> (Lisi,Math,1)
//        11> (Zhangsan,Math,4)
//        1> (Lisi,总分:,3)
//        11> (Zhangsan,总分:,9)
//        1> (Lisi,总分:,6)
//        11> (Zhangsan,总分:,15)


        DataStream<Tuple3<String, String, Integer>> dataStream =  dataStreamSource.keyBy(0).reduce((value1, value2) -> Tuple3.of(value1.f0, "总分", value1.f2 + value2.f2));

        dataStream.print();

        env.execute("Flink Reduce Operator");
    }


}
