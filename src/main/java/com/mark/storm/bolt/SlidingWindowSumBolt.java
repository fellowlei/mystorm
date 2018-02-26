package com.mark.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by lulei on 2018/2/26.
 */
public class SlidingWindowSumBolt extends BaseWindowedBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowSumBolt.class);

    OutputCollector collector;
    int sum = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
       this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expiredTuples = inputWindow.getExpired();

        LOG.debug("Events in current window: " + tuplesInWindow.size());

        for(Tuple tuple: newTuples){
            sum += (int)tuple.getValue(0);
        }
        for(Tuple tuple: expiredTuples){
            sum -= (int)tuple.getValue(0);
        }
        collector.emit(new Values(sum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sum"));
    }
}
