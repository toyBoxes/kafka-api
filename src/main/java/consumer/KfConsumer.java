package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class KfConsumer {
    @Test
    public  void consumerTopic() {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "worker1:9092,worker2:9092,worker3:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> consumerRecords=consumer.poll(1000);
            for (ConsumerRecord<String, String> record : consumerRecords) {

                System.out.println(record);
            }

        }

    }
}
