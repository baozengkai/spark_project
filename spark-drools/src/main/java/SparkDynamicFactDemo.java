import com.xiaobao.db.MysqlPool;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.drools.core.impl.KnowledgeBaseImpl;
import org.kie.api.definition.type.FactType;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.StatelessKieSession;
import org.kie.internal.utils.KieHelper;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import com.alibaba.fastjson.JSONObject;
/**
 * Demo目标：
 *      1.接收kafka的String并映射为json数据(完成)
 *      2.前端配置规则表的原型对象和规则匹配方式
 *      3.后端获取mysql中的规则表，动态创建Fact对象并且根据
 *
 *  动态目标(三个动态)： 比如十分钟之内，同一账号使用2个IP登录。
 *      1.窗口时间动态 : 十分钟是可以改变的
 *      2.规则动态 : 可以选择指定的账号，也可以选择不同次数的IP
 *      3.场景原型动态 : 目前的场景使用的是Person类的userName和ipAddress原型，比如切换到同一个账号，在同一地区使用2个IP登录。
 *
 *  技术支持:
 *      1.窗口时间动态 : spark streaming动态读取配置、使用drools fusion来实现动态配置窗口
 *      2.规则动态 : drools规则引擎匹配 + mysql字符串
 *      3.场景原型动态 : drools规则引擎原型映射 + mysql字符串
 */

public class SparkDynamicFactDemo {
    public static void main(String[] args) throws Exception{
        // 1.创建spark streaming驱动
        SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("SparkDynamicFactDemo");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // 2.设置检查点
        jssc.checkpoint("/tmp/spark");

        // 3.配置Kafka参数，接受Kafka数据
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

        // 4.将string类型转换为JSON类型
        JavaDStream<JSONObject> json_lines = lines.map(new Function<String, JSONObject>() {
            @Override
            public JSONObject call(String jsonStr) throws Exception {
                JSONObject obj = JSONObject.parseObject(jsonStr);
                return obj;
            }
        });

        // 5.JSON类型的键映射为字段字符串保存在数据库中
        json_lines.foreachRDD(new VoidFunction<JavaRDD<JSONObject>>() {
            @Override
            public void call(JavaRDD<JSONObject> listJavaRDD) throws Exception {
                listJavaRDD.foreachPartition(new VoidFunction<Iterator<JSONObject>>() {
                    @Override
                    public void call(Iterator<JSONObject> json_line) throws Exception {
                        // 1.获取连接池连接
                        Connection conn = MysqlPool.getConn();
                        Statement statement = conn.createStatement();

                        String sql = "select drl_package, drl_declare, drl_rule from drools";

                        // 2.获取数据库表的内容
                        ResultSet drlResults = statement.executeQuery(sql);

                        String drlStr = "";
                        while(drlResults.next())
                        {
                            drlStr =  drlResults.getString("drl_Package") +
                                      drlResults.getString("drl_declare") +
                                      drlResults.getString("drl_rule");
                        }
                        KieHelper helper = new KieHelper();
                        helper.addContent(drlStr, ResourceType.DRL);
                        KnowledgeBaseImpl kieBase = (KnowledgeBaseImpl) helper.build();

                        FactType demoType = kieBase.getFactType("rules","Demo");

                        Object demo = demoType.newInstance();

                        StatelessKieSession kieSession = kieBase.newStatelessKieSession();
                        while(json_line.hasNext())
                        {
                            JSONObject line = json_line.next();
                            if(line.containsKey("userName") && line.containsKey("ipAddress"))
                            {
                                demoType.set(demo, "userName", line.get("userName"));
                                demoType.set(demo,"ipAddress", line.get("ipAddress"));
                                kieSession.execute(demo);
                            }
                        }
                    }
                });
            }
        });

        json_lines.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
