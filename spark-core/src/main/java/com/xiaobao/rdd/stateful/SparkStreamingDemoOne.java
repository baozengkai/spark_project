package com.xiaobao.rdd.stateful;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.janino.Java;

import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Spark Streaming有状态操作
 *      1.reduceByKeyAndWindow() 根据键统计计数
 */


public class SparkStreamingDemoOne {
    public static void main(String[] args) throws Exception {
//        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingDemoOne");
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

        JavaPairDStream<String, Long> ipLines = messages.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
           @Override
           public Tuple2<String, Long> call(Tuple2<String, String> t2){
               return new Tuple2<String, Long>(t2._2(), 1L);
           }
        });

        JavaPairDStream<String, Long> ipCountLines = ipLines.reduceByKeyAndWindow((a,b) -> a+b, (a,b)-> a-b, Durations.seconds(30), Durations.seconds(10));
        ipCountLines.print();
        jssc.start();
        jssc.awaitTermination();
    }
}