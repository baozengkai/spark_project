package com.xiaobao.cep;

import com.alibaba.fastjson.JSONObject;
import com.xiaobao.db.MysqlPool;
import com.xiaobao.model.Log;
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
import org.kie.api.KieBaseConfiguration;
import org.kie.api.KieServices;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.definition.type.FactType;
import org.kie.api.internal.utils.KieService;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.StatelessKieSession;
import org.kie.internal.runtime.StatefulKnowledgeSession;
import org.kie.internal.utils.KieHelper;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import com.xiaobao.model.TransactionEvent;

public class SparkDroolsCepDemo {
    public static void main(String[] args) throws Exception
    {
        // 1.创建spark streaming环境
        SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("com.xiaobao.cep.SparkDroolsCepDemo");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

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

                          KieBaseConfiguration kbConfig = KieServices.Factory.get().newKieBaseConfiguration();
                          kbConfig.setOption(EventProcessingOption.STREAM);
                          KnowledgeBaseImpl kieBase = (KnowledgeBaseImpl) helper.build(kbConfig);
                          KieSession kieSession = kieBase.newKieSession();

                          // 4.遍历插入对象，测试是否触发规则引擎
                          while(json_line.hasNext())
                          {
                              JSONObject event = json_line.next();
                              System.out.println("接受到的日志数据为:" + event.toString());

                              TransactionEvent transactionEvent = new TransactionEvent(event.getLong("id"),event.getDouble("amount"));
                              System.out.println(transactionEvent.getExecutionTime());
                              kieSession.insert(transactionEvent);
                              kieSession.fireAllRules();
                          }
                          MysqlPool.releaseConn(conn);
                      }
                  });
              }
        });

        json_lines.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
