package com.xiaobao.rdd;

/**
 *  学习Spark Streaming
 *      创建操作:
 *          1.1 读取socket上数据
 *      转换操作:
 *          2.1 无状态操作filter()过滤
 *      动作操作:
 *          3.1 print() 输出DStream的前十个元素
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;

public class RddDemoThree {
    public static void main(String[] args) throws Exception{
        // 1.创建spark环境
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("RddDemoOne");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 2.接受socket数据流
        JavaDStream<String> lines = jssc.socketTextStream("192.168.42.128", 9999);

        // 3.过滤具有error字段的流
        JavaDStream<String> errorlines = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("error");
            }
        });

        // 4. 打印
        lines.print();

        // 5.启动spark streaming
        jssc.start();
        jssc.awaitTermination();
    }
}
