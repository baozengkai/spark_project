package com.xiaobao.kafka;

/**
 * Kafak生产者生产数据
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerDemo {
    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.42.128:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;

        producer = new KafkaProducer<String, String>(properties);
        for (int i=0; i < 5; i++)
        {
            String msg = "Message" + i;
            producer.send(new ProducerRecord<String, String>("hello", msg));
            System.out.println("Sent: " + msg);
        }
    }
}
