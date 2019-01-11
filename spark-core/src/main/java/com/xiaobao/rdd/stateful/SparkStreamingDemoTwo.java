package com.xiaobao.rdd.stateful;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkStreamingDemoTwo {
    public static void main(String[] args) throws Exception {
        // 1.创建spark streaming驱动
        SparkConf conf = new SparkConf().setMaster("spark://localhost:7077").setAppName("SparkStreamingDemoOne");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // 2.设置检查点
        jssc.checkpoint("/tmp/spark");

        // 3.配置Kafka参数，接受Kafka数据
        String zkQuorum ="192.168.42.129:2181";
        String group = "kafka_Direct";

        Map<String, Integer> topics = new HashMap<>();
        topics.put("hello", 1);

        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topics);

        JavaPairDStream<String, String> ipLines = messages.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> t2){
                List<String> data = Arrays.asList(t2._2().split(" "));
                return new Tuple2<String, String>(data.get(0),data.get(1));
            }
        });

        class AddLongs implements Function2<String, String, String> {
            @Override
            public String call(String v1, String v2) throws Exception {
                if (!v1.equals(v2)) {
                    if (v1.contains(",")) {
                        return v1;
                    }
                    return v1 + "," + v2;
                }
                return v1;
            }
        }
        JavaPairDStream<String, String> ipCountLines = ipLines.reduceByKeyAndWindow(new AddLongs(), Durations.seconds(60), Durations.seconds(10));
        ipCountLines.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
