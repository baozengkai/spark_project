import com.xiaobao.model.Person;
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
import org.kie.api.KieServices;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.xiaobao.model.Product;
import com.xiaobao.model.Log;

public class SparkKafkaDroolsDemo  {
//    static KieServices kieServices = KieServices.Factory.get();
//    static KieContainer kieContainer = kieServices.newKieContainer(kieServices.newReleaseId("com.eisoo.drools",
//            "drools_demo", "1.0-LATEST"));
//    static KieSession kieSession = kieContainer.newKieSession("ksession-rules");

    public static void main(String[] args) throws Exception{
        // 1.创建spark streaming环境
        SparkConf conf = new SparkConf().setAppName("SparkKafkaDroolsDemo");
//         SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkKafkaDroolsDemo");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 2.配置Kafka参数，接受Kafka数据
        String zkQuorum ="192.168.42.128:2181";
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

        StringBuilder drlStr = new StringBuilder("package rules;\nrule test\nwhen\nthen\nSystem.out.println(\"hello strDrl drools!!!\");\nend");


        // 4.针对每一个DStream类型插入到规则引擎中
        logRdd.foreachRDD(new VoidFunction<JavaRDD<Log>>() {
            @Override
            public void call(JavaRDD<Log> logJavaRDD) throws Exception {
                logJavaRDD.foreach(new VoidFunction<Log>() {
                    @Override
                    public void call(Log log) throws Exception {
                        if(log != null){
//                            System.out.println("接收到的日志信息message :" + log.getMessage());
//
//                            KieServices kieServices = KieServices.Factory.get();
//                            KieContainer kieContainer = kieServices.newKieClasspathContainer();
//                            KieSession kieSession = kieContainer.newKieSession("ksession-rules");
//                            System.out.println(System.identityHashCode(kieSession));
//
//                            Log factLog = new Log(log.getMessage());
//                            kieSession.insert(factLog);
//                            kieSession.fireAllRules();

                            // 2.触发指定kjar的规则
//                            System.out.println("接收到的日志信息message :" + log.getMessage());
//                            System.out.println("KieServer的内存: " + System.identityHashCode(kieServices));
//                            System.out.println("KieContainer的内存：" + System.identityHashCode(kieContainer));
//                            System.out.println("KieSession的内存：" + System.identityHashCode(kieSession));
//
//                            Log factLog = new Log(log.getMessage());
//                            kieSession.insert(factLog);
//                            kieSession.fireAllRules();

//                            // 3.验证字符串触发规则引擎
                            if (log.getMessage().contains("error")){
                                drlStr.replace(58,62, "12354");
                                System.out.println("字符串发生了变化:");
                            }
                            System.out.println("收到的日志:" + log.getMessage());
                            System.out.println("此时的drl字符串为: "+drlStr);

                            KieHelper helper = new KieHelper();
                            helper.addContent(drlStr.toString(), ResourceType.DRL);

                            KnowledgeBaseImpl kieBase = (KnowledgeBaseImpl) helper.build();
                            KieSession kieSession = kieBase.newKieSession();
                            Log factLog = new Log(log.getMessage());
                            kieSession.insert(factLog);
                            kieSession.fireAllRules();
                        }
                    }
                });
            }
        });
        lines.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
