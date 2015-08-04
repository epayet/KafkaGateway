package com.cdiscount.kafka;

import com.cdiscount.kafka.gateway.BrokerGateway;
import com.cdiscount.kafka.gateway.ConsumerGateway;
import com.cdiscount.kafka.gateway.ConsumerTopicGateway;
import com.cdiscount.kafka.gateway.TopicGateway;
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
        //Example with one existing topic
//        while(true) {
//            List<ConsumerGateway> consumers = getConsumerInfos();
//            ConsumerTopicGateway consumerTopic = consumers.get(0).consumerTopicsGateway.get(0);
//
//            System.out.println("LogSize : " + consumerTopic.logSize);
//            System.out.println("Lag : " + consumerTopic.lag);
//            System.out.println("Offset : " + consumerTopic.offset);
//            System.out.println("-----------------------------------------------");
//
//            Thread.sleep(5000);
//        }

        KafkaGatewayApi kafkaGatewayApi = new KafkaGatewayApi(createApacheZookeeperClient("localhost:2181"), new KafkaApiUtils());
        try {
            List<BrokerGateway> brokers = kafkaGatewayApi.getBrokers();
            List<TopicGateway> topics = kafkaGatewayApi.getTopics();
            List<ConsumerGateway> consumers = kafkaGatewayApi.getConsumers();

            BrokerGateway broker = brokers.get(0);
            System.out.println("Broker 1 : host : " + broker.host + ", id: " + broker.id + ", port: " + broker.port);
        } catch (ZookeeperException e) {
            e.printStackTrace();
        }
    }

    private static List<ConsumerGateway> getConsumerInfos() {
        KafkaGatewayApi kafkaGatewayApi = new KafkaGatewayApi(createApacheZookeeperClient("localhost:2181"), new KafkaApiUtils());
        List<ConsumerGateway> consumersInfo = new ArrayList<ConsumerGateway>();
        try {
            consumersInfo = kafkaGatewayApi.getConsumers();
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
