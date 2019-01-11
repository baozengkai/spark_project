package com.xiaobao.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 *  键值对RDD的用法
 *      创建操作：
 *          1.创建键值对RDD
 *      转换操作：
 *          2.1 reduceByKey(func) : 聚合具有相同建的二元组
 *          2.2 groupByKey() : 对二元组进行分组
 */

public class RddDemoTwo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("RddDemoOne");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer>  data = Arrays.asList(1,2,3,4,5,1);
        JavaRDD<Integer> lines = sc.parallelize(data);

        // 1.创建键值对RDD
        JavaPairRDD<Integer,Integer> pairRDD = lines.mapToPair(new PairFunction<Integer, Integer, Integer>(){
            @Override
            public Tuple2<Integer, Integer> call(Integer i) throws Exception {
                return new Tuple2(i, 1);
            }
        });

        // 2.1 reduceByKey()聚合操作
        JavaPairRDD<Integer, Integer> reducePairRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        // 2.2 groupByKey()聚合操作
        JavaPairRDD<Integer, Iterable<Integer>> groupPairRDD = pairRDD.groupByKey();

        // 3.输出键值对RDD
        pairRDD.foreach(x -> System.out.println(x));
        reducePairRDD.foreach(x -> System.out.println(x));
        groupPairRDD.foreach(x -> System.out.println(x));
    }
}
