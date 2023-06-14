package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SyncProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties p=new Properties();
        //p.setProperty("key1","hello kafka!");
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"worker1:9092,worker2:9092,worker3:9092");
        //key的序列化
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer .class.getName());
        //value的序列化
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer kafkaProducer=new KafkaProducer<String,String>(p);
        for (int i = 0; i < 5; i++) {

            //同步发送
            //同步发送
            kafkaProducer.send(new ProducerRecord("first", "同步发送消息!"), new Callback() {
                @Override
                public void onCompletion(RecordMetadata record, Exception e) {
                    if(e==null){
                        System.out.println(record.topic()+"-"+record.partition()+"-"+"消息发送成功");
                    }else{
                        System.out.println(e);
                    }
                }
            }).get(); //同步发送
        }
        //kafkaProducer.send(new ProducerRecord())
        kafkaProducer.close();
    }

}
