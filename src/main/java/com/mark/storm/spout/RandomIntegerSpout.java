package com.mark.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * Created by Administrator on 2018/2/25.
 */
public class RandomIntegerSpout extends BaseRichSpout{

    private static final Logger LOG = LoggerFactory.getLogger(RandomIntegerSpout.class);
    private SpoutOutputCollector spoutOutputCollector;
    private Random random;
    private long msgId = 0;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("value","ts","msgid"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        spoutOutputCollector.emit(new Values(random.nextInt(1000),System.currentTimeMillis() - (24 * 60 * 60 * 1000), ++msgId),msgId);
    }

    @Override
    public void ack(Object msgId) {
        LOG.debug("Got ACK for msgId : " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("Got FAIL for msgId : " + msgId);
    }
}
