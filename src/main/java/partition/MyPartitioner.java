package partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
//自定义分区器
public class MyPartitioner implements Partitioner {
    //根据消息内容发送到指定分区
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {

        int partition;
        String msg=o1.toString();
        if(msg.contains("kafka")){
            partition=0;
        }else{
            partition=1;
        }
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
