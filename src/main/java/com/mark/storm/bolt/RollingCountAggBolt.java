package com.mark.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lulei on 2018/2/26.
 */
public class RollingCountAggBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    private Map<Object,Map<Integer,Long>> counts = new HashMap<Object, Map<Integer, Long>>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Object obj = tuple.getValue(0);
        Long count = tuple.getLong(1);
        int source = tuple.getSourceTask();
        Map<Integer, Long> subCounts = counts.get(obj);
        if(subCounts == null){
            subCounts = new HashMap<Integer, Long>();
            counts.put(obj,subCounts);
        }

        subCounts.put(source,count);
        long sum = 0;
        for(Long val: subCounts.values()){
            sum += val;
        }
        outputCollector.emit(new Values(obj,sum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("obj","count"));
    }
}
