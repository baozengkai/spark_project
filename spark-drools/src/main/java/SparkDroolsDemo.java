//
//import com.xiaobao.model.Person;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//import org.apache.spark.*;
//import org.apache.spark.api.java.function.*;
//import org.apache.spark.streaming.*;
//import org.apache.spark.streaming.api.java.*;
//import org.spark_project.guava.io.Files;
//import scala.Tuple2;
//
//import org.kie.api.definition.KiePackage;
//import org.kie.api.io.ResourceType;
//import org.kie.api.runtime.StatelessKieSession;
//import org.kie.internal.builder.KnowledgeBuilder;
//import org.kie.internal.builder.KnowledgeBuilderFactory;
//import org.kie.internal.io.ResourceFactory;
//
//import org.drools.core.impl.InternalKnowledgeBase;
//import org.drools.core.impl.KnowledgeBaseFactory;
//
//import org.kie.api.KieServices;
//import org.kie.api.runtime.KieContainer;
//import org.kie.api.runtime.KieSession;
//
//import java.io.File;
//import java.net.URLDecoder;
//import java.nio.charset.Charset;
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.util.Arrays;
//import java.util.Collection;
//
//
//public class SparkDroolsDemo {
//    public static void main(String[] args) throws Exception{
//
//        // 1.创建spark streaming环境
//        SparkConf conf = new SparkConf().setMaster("spark://master:7077").setAppName("SparkDroolsDemo");
////        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkDroolsDemo");
//        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
//
//        // 2.获取tcp服务器传送的数据
//        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
//
//        // 3.将数据转换为Person类型存入到JavaDStream中
//        JavaDStream<Person> mapRdd = lines.map(new Function<String, Person>() {
//            @Override
//            public Person call(String s) throws Exception {
//                String[] personArray = s.split(" ");
//                if(personArray.length == 2){
//                    Person person = new Person();
//                    person.setName(personArray[0]);
//                    person.setCount(Integer.parseInt(personArray[1]));
//                    return person;
//                }
//                return null;
//            }
//        });
//
////        // 4.针对每一个DStream类型插入到规则引擎中
//        mapRdd.foreachRDD(new VoidFunction<JavaRDD<Person>>() {
//            @Override
//            public void call(JavaRDD<Person> personJavaRDD) throws Exception {
//                personJavaRDD.foreach(new VoidFunction<Person>() {
//                    @Override
//                    public void call(Person person) throws Exception {
//                        if(person != null){
//                            System.out.println("id :" + person.getName());
//                            System.out.println("price : " + person.getCount());
//
//                            KieServices kieServices = KieServices.Factory.get();
//
//                            KieContainer kieContainer = kieServices.newKieClasspathContainer();
//
//                            KieSession kieSession = kieContainer.newKieSession("ksession-rules");
//
//                            Person testPerson = new Person(person.getName(), person.getCount());
//
//                            kieSession.insert(testPerson);
//                            kieSession.fireAllRules();
//                        }
//                    }
//                });
//            }
//        });
//
//        jssc.start();
//        jssc.awaitTermination();
//    }
//}
