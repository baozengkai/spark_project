import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.drools.core.impl.KnowledgeBaseImpl;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.StatelessKieSession;
import org.kie.internal.utils.KieHelper;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.xiaobao.db.MysqlPool;
import com.xiaobao.model.Log;


public class SparkKafkaMysqlDroolsDemo  {

    public static void main(String[] args) throws Exception{
        // 1.创建spark streaming环境
        SparkConf conf = new SparkConf().setMaster("spark://localhost:7077").setAppName("SparkKafkaMysqlDroolsDemo");
//        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkKafkaDroolsDemo");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 2.配置Kafka参数，接受Kafka数据
        String zkQuorum ="192.168.42.129:2181";
        String group = "kafka_Direct";

        Map<String, Integer> topics = new HashMap<>();
        topics.put("hello", 1);

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topics);

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                return tuple2._2();
            }
        });

        // 3.将数据转换为Person类型存入到JavaDStream中
        JavaDStream<Log> logRdd = lines.map(new Function<String, Log>() {
            @Override
            public Log call(String line) throws Exception {
                Log log = new Log();
                log.setMessage(line);
                return log;
            }
        });

        // 4.使用foreachPartition，为每一个partition创建一个mysql的句柄
        logRdd.foreachRDD(new VoidFunction<JavaRDD<Log>>() {
            @Override
            public void call(JavaRDD<Log> logJavaRDD) throws Exception {
                logJavaRDD.foreachPartition(new VoidFunction<Iterator<Log>>() {
                    @Override
                    public void call(Iterator<Log> logIterator) throws Exception {
                        // 1.获取连接池连接
                        Connection conn = MysqlPool.getConn();
                        Statement statement = conn.createStatement();

                        String sql = "select drl from drools";

                        // 2.获取数据库表的内容
                        ResultSet drlResults = statement.executeQuery(sql);
                        int columns = drlResults.getMetaData().getColumnCount();

                        String drlStr = "";
                        while(drlResults.next())
                        {
                            for(int i = 1;i <= columns; i++)
                            {
                                drlStr = drlResults.getString(i);
                            }
                        }
                        // 3. 添加Drools对象
                        KieHelper helper = new KieHelper();
                        helper.addContent(drlStr, ResourceType.DRL);
                        KnowledgeBaseImpl kieBase = (KnowledgeBaseImpl) helper.build();
                        StatelessKieSession kieSession = kieBase.newStatelessKieSession();

                        // 4.遍历插入对象，测试是否触发规则引擎
                        while(logIterator.hasNext())
                        {
                            Log log = logIterator.next();
                            System.out.println("接受到的日志数据为:" + log.getMessage());
                            Log factLog = new Log(log.getMessage());
                            kieSession.execute(factLog);
                        }
                        MysqlPool.releaseConn(conn);
                    }
                });
            }
        });
        lines.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
