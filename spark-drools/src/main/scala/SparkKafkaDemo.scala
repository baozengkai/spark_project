import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import kafka.serializer.StringDecoder


object SparkKafkaDemo {
  def main(args: Array[String]): Unit = {
    // 1、配置Spark Streaming对象
//    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkKafkaDemo")
    val conf = new SparkConf().setAppName("SparkKafkaDemo")
    val ssc = new StreamingContext(conf, Seconds(5))

    //2、配置kafka相关参数
    val kafkaParams = Map("metadata.broker.list" -> "192.168.42.128:9092", "group.id" -> "Kafka_Direct")

    //3、定义topic
    val topics = Set("hello")

    //4、通过 KafkaUtils.createDirectStream接受kafka数据，这里采用是kafka低级api偏移量不受zk管理
    val dstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    //5、获取kafka中topic中的数据
    val topicData: DStream[String] = dstream.map(_._2)
    //6、切分每一行,每个单词计为1
    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_, 1))
    //7、相同单词出现的次数累加
    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    //8、打印输出
    result.print()

    //9、开启spark streaming计算
    ssc.start()
    ssc.awaitTermination()
  }
}
