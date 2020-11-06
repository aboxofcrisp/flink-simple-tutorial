package sql.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class SqlConnect {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        // 创建一个使用 Blink Planner 的 TableEnvironment, 并工作在流模式
        TableEnvironment tEnv = TableEnvironment.create(settings);

        String kafkaSourceSql = "CREATE TABLE log (\n" +
                "    t INT, \n" +
                "    user_name VARCHAR,\n" +
                "    cnt INT\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "    'connector.topic' = 'flink',\n" +
               "    'connector.startup-mode' = 'latest-offset',\n" +
                "    'connector.properties.0.key' = 'group.id',\n" +
                "    'connector.properties.0.value' = 'testGroup',\n" +
                "    'connector.properties.1.key' = 'bootstrap.servers',\n" +
                "    'connector.properties.1.value' = '127.0.0.1:9092',\n" +
                "    'connector.specific-offsets.0.partition' = '0',\n" +
                "    'connector.specific-offsets.0.offset' = '0',\n" +
                "    'update-mode' = 'append',\n" +
                "    'format.type' = 'json',\n" +
                "    'format.derive-schema' = 'true'\n" +
                ")";

        String mysqlSinkSql = "CREATE TABLE sink (\n" +
                "    t INT,\n" +
                "    user_name VARCHAR,\n" +
                "    total INT\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.url' = 'jdbc:mysql://127.0.0.1:3306/flink',\n" +
                "    'connector.table' = 'log',\n" +
                "    'connector.username' = 'root',\n" +
                "    'connector.password' = '12345678',\n" +
                "    'connector.write.flush.max-rows' = '1'\n" +
                ")";

        // 1. 连接kafka构建源表
        tEnv.sqlUpdate(kafkaSourceSql);


        // 2. 定义要输出的表
        tEnv.sqlUpdate(mysqlSinkSql);


        // 3. 自定义具体的 DML 操作

        tEnv.sqlUpdate("INSERT INTO sink " +
                "SELECT * from log where cnt=100");

        tEnv.execute("SQL Job");
    }
}
