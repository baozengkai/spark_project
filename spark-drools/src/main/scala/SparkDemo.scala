import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext._

object SparkDemo {
  def main(args: Array[String]): Unit = {

    // 1.spark demo1
//    val conf = new SparkConf().setMaster("spark://master:7077").setAppName("SparkDemo")
//    val sc = new SparkContext(conf)
//    val a = sc.parallelize(List(1, 2, 3, 4))
//    a.persist()
//    println(a.count())
//    println("============================")
//    a.collect().foreach(println)

    // 2.spark streaming demo1
    val conf = new SparkConf().setAppName("SparkDemo")
    val ssc = new StreamingContext(conf, Seconds(1))

    // 创建一个获取tcp数据源的DStream
    val lines = ssc.socketTextStream("localhost", 9999)
    // 将每一行分割成多个单词
    val words = lines.flatMap(_.split(" "))

    // 对每一批次的单词进行计数
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_+_)

    // 将DStream产生的RDD的头10个元素打印出来
    wordCounts.print()

    // 启动流式计算
    ssc.start()
    // 等待知道计算终止
    ssc.awaitTermination()
  }
}
