package dataSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class InsideDataSource {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        // 添加数组作为数据输入源
//        String[] elementInput = new String[]{"hello Flink", "Second Line"};
//        DataStream<String> text = env.fromElements(elementInput);


        DataStream<String> textLines =  env.readTextFile("/Users/sutaobian/github/flink-simple-tutorial/LICENSE");
//        textLines.print();
        Map<String,Integer> hashMap  = new HashMap();
        DataStream<Tuple2<String,Integer>> parsed = textLines.map(new MapFunction<String, Tuple2<String,Integer>>() {

            @Override
            public Tuple2 map(String value) {
                Tuple2 tuple2 = new Tuple2(value,value.length());
                return tuple2;
            }
        });

        parsed.writeAsCsv  ("/tmp/flink-output", FileSystem.WriteMode.OVERWRITE);

//        // 添加List集合作为数据输入源
//        List<String> collectionInput = new ArrayList<>();
//        collectionInput.add("hello Flink");
//        DataStream<String> text2 = env.fromCollection(collectionInput);
//
//        // 添加Socket作为数据输入源
//        // 4个参数 -> (hostname:Ip地址, port:端口, delimiter:分隔符, maxRetry:最大重试次数)
//        DataStream<String> text3 = env.socketTextStream("localhost", 9999, "\n", 4);
//
//
//        // 添加文件源
//        // 直接读取文本文件
//        DataStream<String> text4 = env.readTextFile("/opt/history.log");
//        // 指定 CsvInputFormat, 监控csv文件(两种模式), 时间间隔是10ms
//        DataStream<String> text5 = env.readFile(new CsvInputFormat<String>(new Path("/opt/history.csv")) {
//            @Override
//            protected String fillRecord(String s, Object[] objects) {
//                return null;
//            }
//        },"/opt/history.csv", FileProcessingMode.PROCESS_CONTINUOUSLY,10);

       // text.print();

        env.execute("Inside DataSource Demo");

        System.out.println("..........end");
    }
}
