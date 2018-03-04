package com.mark.storm.mapreduce;

import com.mark.storm.bolt.PrinterBolt;
import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FileSentenceSpout extends BaseRichSpout{
    SpoutOutputCollector spoutOutputCollector;
    List<String> lines = null;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        try {
            this.lines = FileUtils.readLines(new File("d:/log.txt"), "utf-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout",new FileSentenceSpout(),1);
        topologyBuilder.setBolt("map",new MapBolt(),1).shuffleGrouping("spout");
        topologyBuilder.setBolt("reduce",new ReduceBolt(),1).shuffleGrouping("map");
        topologyBuilder.setBolt("print",new PrinterBolt(),1).shuffleGrouping("reduce");


        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(1);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("mydemo",config,topologyBuilder.createTopology());

    }
}
