package com.xiaobao.rdd;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 学习Java操作RDD
 *      创建RDD操作:
 *          1.1 利用parallelize方法转换List集合为RDD
 *          1.2 利用testFile()方法读取文件文件为RDD
 *      转换RDD操作:
 *          2.1 fiter()转换
 *          2.2 map()转换
 *      行动操作
 *          3.1 foreach()输出
 *          3.2 reduce()聚合
 */

public class RddDemoOne {
    public static void main(String[] args){
        SparkConf  conf = new SparkConf().setMaster("local[2]").setAppName("RddDemoOne");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1.创建RDD
        List<String> data = Arrays.asList("error","exception","123");
        JavaRDD<String> lines = sc.parallelize(data);

        // 2.转换RDD
        // 2.1 filter()转换
        JavaRDD<String> errorRDD1 = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("error");
            }
        });

        // 2.1 filter()转换lambda形式
        JavaRDD<String> errorRDD2 = lines.filter(s->s.contains("error"));

        // 2.2 map()转换
        JavaRDD<String> mapRDD1 = lines.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s + "1";
            }
        });

        JavaRDD<String> mapRDD2 = lines.map(s -> s + "2");

        // 3.1 foreach()输出
        errorRDD1.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        // 3.1 foreach()的lambda形式输出
        errorRDD2.foreach(x -> System.out.println(x));
        mapRDD1.foreach(x -> System.out.println(x));
        mapRDD2.foreach(x -> System.out.println(x));

        // 3.2 reduce()
        String str = lines.reduce(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s + s2;
            }
        });

        System.out.println(str);
    }
}
