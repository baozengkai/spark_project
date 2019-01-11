package com.xiaobao.cases;

import com.xiaobao.db.MysqlPool;
import com.xiaobao.model.Log;
import com.xiaobao.model.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
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
import java.util.*;

import com.xiaobao.model.Person;

/**
 *  公司场景一：
 *      同一账号在1分钟之内用2个不同的IP访问
 */

public class SparkDroolsCaseOne
{
    public static void main(String[] args) throws  Exception{
        // 1.创建spark streaming驱动
        SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("SparkDroolsCaseOne");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // 2.设置检查点
        jssc.checkpoint("/tmp/spark");

        // 3.配置Kafka参数，接受Kafka数据
        String zkQuorum ="192.168.84.150:2181";
        String group = "kafka_Direct";

        Map<String, Integer> topics = new HashMap<>();
        topics.put("hello", 1);

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topics);

        JavaPairDStream<String, List<String>> ipLines = messages.mapToPair(new PairFunction<Tuple2<String, String>, String, List<String>>() {
            @Override
            public Tuple2<String, List<String>> call(Tuple2<String, String> t2){
                List<String> data = Arrays.asList(t2._2().split(" "));
                List<String> newData = new ArrayList<String>(data);
                newData.remove(0);
                return new Tuple2<String, List<String>>(data.get(0),newData);
            }
        });

        class AddListStrings implements  Function2<List<String>, List<String>, List<String>>
        {
            @Override
            public List<String> call(List<String> v1, List<String> v2) throws Exception {
                for(int i = 0; i < v2.size(); i++)
                {
                    if(!v1.contains(v2.get(i)))
                    {
                        v1.add(v2.get(i));
                        return v1;
                    }
                }
                return v1;
            }
        }
        JavaPairDStream<String, List<String>> ipCountLines = ipLines.reduceByKeyAndWindow(new AddListStrings(), Durations.seconds(30), Durations.seconds(10));

        JavaDStream<Person> personDS = ipCountLines.map(new Function<Tuple2<String, List<String>>, Person>() {
            @Override
            public Person call(Tuple2<String, List<String>> tuple2) throws Exception {
                Person person = new Person(tuple2._1(), tuple2._2());
                return person;
            }
        });

        // 4.使用foreachPartition，为每一个partition创建一个mysql的句柄
        personDS.foreachRDD(new VoidFunction<JavaRDD<Person>>() {
            @Override
            public void call(JavaRDD<Person> personJavaRDD) throws Exception {
                personJavaRDD.foreachPartition(new VoidFunction<Iterator<Person>>() {
                    @Override
                    public void call(Iterator<Person> personIterator) throws Exception {
                        // 1.获取连接池连接
                        Connection conn = MysqlPool.getConn();
                        Statement statement = conn.createStatement();

                        String sql = "select drl from drools";

                        // 2.获取数据库表的内容
                        ResultSet drlResults = statement.executeQuery(sql);
                        int columns = drlResults.getMetaData().getColumnCount();

//                        List<String> drlStrs = new ArrayList<>();
                        String drlStr="";
                        while(drlResults.next())
                        {
                            for(int i = 1;i <= columns; i++)
                            {
//                                drlStrs.add (drlResults.getString(i));
                                drlStr = drlResults.getString(i);
                            }
                        }

                        // 3. 添加Drools对象
                        KieHelper helper = new KieHelper();
                        helper.addContent(drlStr, ResourceType.DRL);
//                        for(String drlStr : drlStrs) {
//                            helper.addContent(drlStr, ResourceType.DRL);
//                        }
                        KnowledgeBaseImpl kieBase = (KnowledgeBaseImpl) helper.build();

                        StatelessKieSession kieSession = kieBase.newStatelessKieSession();

                        // 4.遍历插入对象，测试是否触发规则引擎
                        while(personIterator.hasNext())
                        {
                            Person person = personIterator.next();
                            System.out.println("聚合后的日志数据为:" + person.getUserName() + " " +person.getIpAddress());
                            Person insertPerson = new Person(person.getUserName(), person.getIpAddress());
                            kieSession.execute(insertPerson);
                        }
                        MysqlPool.releaseConn(conn);
                    }
                });
            }
        });
        ipCountLines.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
