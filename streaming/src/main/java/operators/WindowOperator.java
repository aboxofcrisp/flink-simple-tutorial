package operators;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class WindowOperator {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        List data = new ArrayList<Tuple3<Integer,Integer,Integer>>();
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,5,6));
        data.add(new Tuple3<>(0,3,5));
        data.add(new Tuple3<>(1,1,9));
        data.add(new Tuple3<>(1,2,8));
        data.add(new Tuple3<>(1,3,10));
        data.add(new Tuple3<>(1,2,9));

        DataStreamSource<Tuple3<Integer,Integer,Integer>> items = env.fromCollection(data);
        items.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5)));
        items.print();

        env.execute();
    }
}
