package com.cdiscount.kafka;

import com.cdiscount.kafka.info.BrokerInfo;
import com.cdiscount.kafka.info.ConsumerInfo;
import com.cdiscount.kafka.info.TopicInfo;
import com.cdiscount.kafka.utils.KafkaApiUtils;
import com.cdiscount.kafka.zookeeper.ApacheZookeeperClient;
import com.cdiscount.kafka.zookeeper.ZookeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by emmanuel_payet on 27/11/14.
 */
public class Main {
    public static void main (String[] args) throws InterruptedException {
//        while(true) {
//            List<ConsumerInfo> consumersInfo = getConsumerInfos();
//            ConsumerTopicInfo consumerTopicInfo = consumersInfo.get(0).topicsInfo.get(0);
//
//            System.out.println("LogSize : " + consumerTopicInfo.logSize);
//            System.out.println("Lag : " + consumerTopicInfo.lag);
//            System.out.println("Offset : " + consumerTopicInfo.offset);
//            System.out.println("-----------------------------------------------");
//
//            Thread.sleep(5000);
//        }

        KafkaInfoApi kafkaInfoApi = new KafkaInfoApi(createApacheZookeeperClient("A01KAFKA002.cdweb.biz:2181"), new KafkaApiUtils());
        try {
            List<BrokerInfo> brokers = kafkaInfoApi.getBrokersInfo();
            List<TopicInfo> topics = kafkaInfoApi.getTopicsInfo();
            List<ConsumerInfo> consumers = kafkaInfoApi.getConsumersInfo();
            System.out.println("-----------------------------------------------");
        } catch (ZookeeperException e) {
            e.printStackTrace();
        }
    }

    private static List<ConsumerInfo> getConsumerInfos() {
        KafkaInfoApi kafkaInfoApi = new KafkaInfoApi(createApacheZookeeperClient("A01KAFKA002.cdweb.biz:2181"), new KafkaApiUtils());
        List<ConsumerInfo> consumersInfo = new ArrayList<ConsumerInfo>();
        try {
            consumersInfo = kafkaInfoApi.getConsumersInfo();
        } catch (ZookeeperException e) {
            e.printStackTrace();
        }
        return consumersInfo;
    }

    private static ApacheZookeeperClient createApacheZookeeperClient(String connectionString) {
        ApacheZookeeperClient zookeeperClient = null;
        try {
            zookeeperClient = new ApacheZookeeperClient(connectionString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return zookeeperClient;
    }
}
