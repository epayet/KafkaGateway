package com.cdiscount.kafka.info;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by emmanuel_payet on 27/11/14.
 */
public class TopicInfo {
    public String name;
    public List<Integer> partitionsIds;
    public int leaderId;
}
