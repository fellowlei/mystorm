package com.mark.storm;

import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lulei on 2018/2/26.
 */
public class SkewedRollingTopWords {
    private static final Logger LOG = LoggerFactory.getLogger(SkewedRollingTopWords.class);
    private static final int TOP_N = 5;

    public static void main(String[] args) {
        TopologyBuilder builder =new TopologyBuilder();
        builder.setSpout("spout",new TestWordSpout(),1);
    }
}
