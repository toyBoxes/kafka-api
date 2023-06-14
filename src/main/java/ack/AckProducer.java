package ack;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AckProducer {
    public static void main(String[] args) {
        Properties p=new Properties();
        //p.setProperty("key1","hello kafka!");
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"worker1:9092,worker2:9092,worker3:9092");
        //key的序列化
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //value的序列化
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //设置缓冲区大小
        p.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554422);
        //设置批次大小
        p.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        //设置等待时间
        p.put(ProducerConfig.LINGER_MS_CONFIG,1);
        //设置压缩类型
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        //ack设置为-1
        p.put(ProducerConfig.ACKS_CONFIG,"-1");
        //幂等性,可不写，默认为true
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");

        p.put(ProducerConfig.RETRIES_CONFIG,"10");
        KafkaProducer kafkaProducer=new KafkaProducer<String,String>(p);
        for (int i = 0; i < 5; i++) {
            //异步发送
            kafkaProducer.send(new ProducerRecord("first","acktest"));
        }
        kafkaProducer.close();
    }
}
