package dataSource;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;


public class FileSource {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String filePath = "/tmp/input";
        FileProcessingMode watchType = FileProcessingMode.PROCESS_CONTINUOUSLY;
        long interval = 10;
        FileInputFormat  inputFormat = new TextInputFormat(new Path(filePath));
        DataStreamSource<String>  dataStreamSource =  env.readFile (inputFormat,filePath,watchType,interval);
        dataStreamSource.print().setParallelism(1);


        env.execute("Inside DataSource Demo");

    }
}
