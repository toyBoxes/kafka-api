package transation;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransationProducer {
    public static void main(String[] args) {
        Properties p=new Properties();
        //p.setProperty("key1","hello kafka!");
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"worker1:9092,worker2:9092,worker3:9092");
        //key的序列化
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //value的序列化
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //事务id全局唯一
        p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transation-producer");
        KafkaProducer kafkaProducer=new KafkaProducer<String,String>(p);
        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();
        try{
        for (int i = 0; i < 5; i++) {
            //异步发送
            kafkaProducer.send(new ProducerRecord("first","transation"+i,"hello kafka!"));
        }
            kafkaProducer.commitTransaction();
        }catch (Exception e){
            kafkaProducer.abortTransaction();
        }finally {
            kafkaProducer.close();
        }
    }
}
