package com.cdiscount.kafka;

import com.cdiscount.kafka.gateway.BrokerGateway;
import com.cdiscount.kafka.gateway.ConsumerGateway;
import com.cdiscount.kafka.gateway.ConsumerTopicGateway;
import com.cdiscount.kafka.gateway.TopicGateway;
import com.cdiscount.kafka.utils.KafkaApiUtils;
import com.cdiscount.kafka.zookeeper.ApacheZookeeperClient;
import com.cdiscount.kafka.zookeeper.ZookeeperClient;
import com.cdiscount.kafka.zookeeper.ZookeeperException;
import kafka.javaapi.consumer.SimpleConsumer;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by emmanuel_payet on 27/11/14.
 * TODO : Get the other partitions
 */
public class KafkaGatewayApi {
    private ZookeeperClient zookeeperClient;
    private KafkaApiUtils kafkaApiUtils;

    public KafkaGatewayApi() {
    }

    public KafkaGatewayApi(ZookeeperClient zookeeperClient, KafkaApiUtils kafkaApiUtils) {
        this.zookeeperClient = zookeeperClient;
        this.kafkaApiUtils = kafkaApiUtils;
    }

    public KafkaGatewayApi(String zookeeperAddress) throws ZookeeperException {
        this.zookeeperClient = new ApacheZookeeperClient(zookeeperAddress);
        this.kafkaApiUtils = new KafkaApiUtils();
    }

    public List<ConsumerGateway> getConsumers() throws ZookeeperException {
        List<ConsumerGateway> consumersGateway = getConsumersGatewayFromZookeeper();
        for (ConsumerGateway consumerGateway : consumersGateway) {
            for (ConsumerTopicGateway consumerTopicGateway : consumerGateway.consumerTopicsGateway) {
                consumerTopicGateway.logSize = getLogSize(consumerGateway.name, consumerTopicGateway.name);
                consumerTopicGateway.lag = consumerTopicGateway.logSize - consumerTopicGateway.offset;
            }
        }
        return consumersGateway;
    }

    public List<TopicGateway> getTopics() throws ZookeeperException {
        List<TopicGateway> topicsInfo = new ArrayList<TopicGateway>();
        List<String> topicNames = zookeeperClient.getChildren("/brokers/topics");
        for (String topicName : topicNames) {
            TopicGateway topicGateway = new TopicGateway();
            topicGateway.name = topicName;

            topicGateway.partitionsIds = new ArrayList<Integer>();
            List<String> partitions = zookeeperClient.getChildren("/brokers/topics/" + topicName + "/partitions");
            for (String partition : partitions) {
                topicGateway.partitionsIds.add(Integer.parseInt(partition));
            }

            JSONObject data = zookeeperClient.getJsonData("/brokers/topics/" + topicName + "/partitions/0/state");
            topicGateway.leaderId = data.getInt("leader");

            topicsInfo.add(topicGateway);
        }

        return topicsInfo;
    }

    public List<BrokerGateway> getBrokers() throws ZookeeperException {
        List<BrokerGateway> brokersInfo = new ArrayList<BrokerGateway>();

        List<String> brokersIds = zookeeperClient.getChildren("/brokers/ids");
        for (String brokerId : brokersIds) {
            BrokerGateway brokerGateway = new BrokerGateway();
            brokerGateway.id = Integer.parseInt(brokerId);
            JSONObject data = zookeeperClient.getJsonData("/brokers/ids/" + brokerId);
            brokerGateway.host = data.getString("host");
            brokerGateway.port = data.getInt("port");
            brokersInfo.add(brokerGateway);
        }

        return brokersInfo;
    }

    public long getLogSize(String consumerName, String topicName) throws ZookeeperException {
        BrokerGateway brokerGateway = getLeaderBroker(topicName);
        SimpleConsumer kafkaConsumer = kafkaApiUtils.createSimpleConsumer(brokerGateway.host, brokerGateway.port, topicName);
        return kafkaApiUtils.getLastOffset(kafkaConsumer, topicName, 0, kafkaApiUtils.getOffsetLatestTime(), consumerName);
    }

    public BrokerGateway getLeaderBroker(String topicName) throws ZookeeperException {
        List<TopicGateway> topicsInfo = getTopics();
        int leaderId = 0;
        for(TopicGateway topicGateway : topicsInfo) {
            if (topicGateway.name.equals(topicName)) {
                leaderId = topicGateway.leaderId;
            }
        }
        List<BrokerGateway> brokersInfo = getBrokers();
        for (BrokerGateway brokerGateway : brokersInfo) {
            if (brokerGateway.id == leaderId) {
                return brokerGateway;
            }
        }
        return null;
    }

    private List<ConsumerGateway> getConsumersGatewayFromZookeeper() throws ZookeeperException {
        List<String> consumersNames = zookeeperClient.getChildren("/consumers");
        List<ConsumerGateway> consumersInfo = new ArrayList<ConsumerGateway>();

        for (String name : consumersNames) {
            ConsumerGateway consumerGateway = new ConsumerGateway();
            consumerGateway.name = name;
            consumerGateway.consumerTopicsGateway = createConsumerTopicsGateway(consumerGateway.name);
            consumersInfo.add(consumerGateway);
        }

        return consumersInfo;
    }

    private List<ConsumerTopicGateway> createConsumerTopicsGateway(String consumerName) throws ZookeeperException {
        List<ConsumerTopicGateway> consumerTopicsInfo = new ArrayList<ConsumerTopicGateway>();
        List<String> consumerTopics = zookeeperClient.getChildren("/consumers/" + consumerName + "/offsets");
        for (String topic : consumerTopics) {
            ConsumerTopicGateway consumerTopicGateway = new ConsumerTopicGateway();
            consumerTopicGateway.name = topic;
            consumerTopicGateway.offset = getConsumerTopicOffset(consumerName, topic);
            consumerTopicsInfo.add(consumerTopicGateway);
        }
        return consumerTopicsInfo;
    }

    private long getConsumerTopicOffset(String consumerName, String topic) throws ZookeeperException {
        String consumerTopicOffset = zookeeperClient.getData("/consumers/" + consumerName + "/offsets/" + topic + "/0");
        return Long.parseLong(consumerTopicOffset);
    }

    public void setKafkaApiUtils(KafkaApiUtils kafkaApiUtils) {
        this.kafkaApiUtils = kafkaApiUtils;
    }

    public void setZookeeperClient(ZookeeperClient zookeeperClient) {
        this.zookeeperClient = zookeeperClient;
    }
}
