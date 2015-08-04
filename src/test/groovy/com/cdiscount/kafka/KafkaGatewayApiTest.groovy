package com.cdiscount.kafka

import com.cdiscount.kafka.gateway.BrokerGateway
import com.cdiscount.kafka.gateway.ConsumerGateway
import com.cdiscount.kafka.gateway.ConsumerTopicGateway
import com.cdiscount.kafka.utils.KafkaApiUtils
import com.cdiscount.kafka.zookeeper.MockZookeeperClient
import org.json.JSONObject
import spock.lang.Specification

/**
 * Created by emmanuel_payet on 27/11/14.
 */
class KafkaGatewayApiTest extends Specification {

    def kafkaGatewayApi

    void setup() {
        kafkaGatewayApi = new KafkaGatewayApi(createSimpleZookeeperClientMock(), createSimpleKafkaApiUtilsMock())
    }


    def "GetConsumers"() {
        given:
        ConsumerGateway expectedConsumerGateway = createSimpleExpectedConsumerGateway()

        when:
        def consumersGateway = kafkaGatewayApi.getConsumers()

        then:
        consumersGateway[0].name == expectedConsumerGateway.name
        consumersGateway[0].consumerTopicsGateway[0].offset == expectedConsumerGateway.consumerTopicsGateway[0].offset
        consumersGateway[0].consumerTopicsGateway[0].logSize == expectedConsumerGateway.consumerTopicsGateway[0].logSize
        consumersGateway[0].consumerTopicsGateway[0].lag == 5
    }

    def "GetLogSize"() {
        when:
        def logSize = kafkaGatewayApi.getLogSize("test", "topic")

        then:
        logSize == 10
    }

    def "GetLeaderBroker"() {
        when:
        BrokerGateway brokerGateway = kafkaGatewayApi.getLeaderBroker("topic")

        then:
        brokerGateway.id == 1
    }

    def "GetTopics"() {
        when:
        def topicsGateway = kafkaGatewayApi.getTopics()

        then:
        topicsGateway[0].name == 'topic'
        topicsGateway[0].leaderId == 1
    }

    def "GetBrokers"() {
        when:
        def brokersGateway = kafkaGatewayApi.getBrokers()

        then:
        brokersGateway[0].host == "host"
        brokersGateway[0].id == 1
        brokersGateway[0].port == 9092
    }

    private ConsumerGateway createSimpleExpectedConsumerGateway() {
        def expectedConsumerGateway = new ConsumerGateway()
        expectedConsumerGateway.name = 'test'
        expectedConsumerGateway.consumerTopicsGateway = new ArrayList<>()

        def consumerTopicGateway = new ConsumerTopicGateway()
        consumerTopicGateway.name = 'topic'
        consumerTopicGateway.offset = 5
        consumerTopicGateway.logSize = 10
        expectedConsumerGateway.consumerTopicsGateway.add(consumerTopicGateway)
        expectedConsumerGateway
    }

    private MockZookeeperClient createSimpleZookeeperClientMock() {
        return Stub(MockZookeeperClient) {
            getChildren("/consumers") >> ['test']
            getChildren("/consumers/test/offsets") >> ['topic']
            getData("/consumers/test/offsets/topic/0") >> '5'

            getChildren("/brokers/topics") >> ['topic']
            getChildren("/brokers/topics/topic/partitions") >> ['0']
            getJsonData("/brokers/topics/topic/partitions/0/state") >> new JSONObject("{'leader': 1}")

            getChildren("/brokers/ids") >> ['1']
            getJsonData("/brokers/ids/1") >> new JSONObject("{'host': 'host', 'port': 9092}")
        }
    }

    private KafkaApiUtils createSimpleKafkaApiUtilsMock() {
        return Stub(KafkaApiUtils) {
            createSimpleConsumer("host", 9092, _) >> null
            getLastOffset(_, "topic", 0, 1, "test") >> 10
            getOffsetLatestTime() >> 1
        }
    }
}
