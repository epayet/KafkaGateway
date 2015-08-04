package com.cdiscount.kafka.gateway;

import java.util.List;

/**
 * Created by emmanuel_payet on 27/11/14.
 */
public class TopicGateway {
    public String name;
    public List<Integer> partitionsIds;
    public int leaderId;
}
