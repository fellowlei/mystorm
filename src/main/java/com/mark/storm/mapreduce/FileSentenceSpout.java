package com.mark.storm.mapreduce;

import com.mark.storm.mapreduce.service.IOService;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * Created by fellowlei on 2018/3/4
 */
public class FileSentenceSpout extends BaseRichSpout{
    SpoutOutputCollector spoutOutputCollector;
    List<String> lines = null;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.lines = IOService.getLines("d:/log.txt");
    }

    @Override
    public void nextTuple() {
        for(String line: lines){
            Utils.sleep(1000);
            spoutOutputCollector.emit(new Values(line));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }


}
