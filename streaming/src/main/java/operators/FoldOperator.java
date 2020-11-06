package operators;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;


public class FoldOperator {

    private static final Logger LOG = LoggerFactory.getLogger(FoldOperator.class);
    private static final String[] TYPE = {"苹果", "梨", "西瓜", "葡萄", "火龙果"};

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<Integer, Double, String>> in = env.fromElements(Tuple3.of(100,99.0,"math"),Tuple3.of(200,89.0,"art"));
        DataStream<Tuple2<String,Integer>> in2 =  in.project(2,0);
        in2.print();



        // 添加自定义数据源,每秒发出一笔订单信息{商品名称,商品数量}
        DataStreamSource<Tuple2<String, Integer>> orderSource = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            private volatile boolean isRunning = true;
            private final Random random = new Random();

            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                while (isRunning) {
                    TimeUnit.SECONDS.sleep(1);
                    String type = TYPE[random.nextInt(TYPE.length)];
                    System.out.println("---------------");
                    System.out.println(type);
                    ctx.collect(Tuple2.of (type, 1));
//                    ctx.collect(Tuple2.of(TYPE[random.nextInt(TYPE.length)], 1));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }

        }, "order-info");


        // 这里只为将DataStream → KeyedStream,用空字符串做分区键。所有数据为相同分区
        orderSource.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
//                return "";
            }
        }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, Map<String, Integer>>() {
            // 这里用HashMap做暂存器
                    @Override
                    public Map fold(Map<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
                        accumulator.put(value.f0, accumulator.getOrDefault(value.f0, 0) + value.f1);
                        return accumulator;
                    }
                })
                .print();

        env.execute("Fold Operator");
    }
}
