package com.cdiscount.kafka.zookeeper;

import org.apache.zookeeper.KeeperException;

/**
 * Created by emmanuel_payet on 27/11/14.
 */
public class ZookeeperException extends Exception {
    public ZookeeperException(Exception e) {
        super(e);
    }
}
