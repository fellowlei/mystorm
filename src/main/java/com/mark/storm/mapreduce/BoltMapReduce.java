package com.mark.storm.mapreduce;

import com.mark.storm.bolt.PrinterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

/**
 * Created by fellowlei on 2018/3/4
 */
public class BoltMapReduce {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout", new FileSentenceSpout(), 1);
        topologyBuilder.setBolt("map", new MapBolt(), 1).shuffleGrouping("spout");
//        topologyBuilder.setBolt("reduce", new ReduceBolt(), 1).shuffleGrouping("map");
        topologyBuilder.setBolt("reduce", new WindowReduceBolt().withWindow(BaseWindowedBolt.Count.of(2), BaseWindowedBolt.Count.of(1)), 1).shuffleGrouping("map");
        topologyBuilder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("reduce");


        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(1);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("mydemo", config, topologyBuilder.createTopology());

    }
}
