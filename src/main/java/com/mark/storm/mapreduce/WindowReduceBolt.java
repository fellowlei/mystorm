package com.mark.storm.mapreduce;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lulei on 2018/3/5.
 */
public class WindowReduceBolt extends BaseWindowedBolt {

    OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        Map<String,Integer> map = new HashMap<>();
        List<Tuple> tuples = tupleWindow.get();
        for(Tuple tuple: tuples){
            String word = tuple.getStringByField("word");
            Integer count = tuple.getIntegerByField("count");
            Integer sum = 0;
            if(map.containsKey(word)){
                sum = count + map.get(word);
                map.put(word,sum);
            }else{
                map.put(word,count);
                sum = count;
            }
            outputCollector.emit(new Values(word,sum));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","sum"));
    }
}
