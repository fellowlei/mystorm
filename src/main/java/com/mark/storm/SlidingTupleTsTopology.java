package com.mark.storm;

import com.mark.storm.bolt.PrinterBolt;
import com.mark.storm.bolt.SlidingWindowSumBolt;
import com.mark.storm.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.util.concurrent.TimeUnit;

/**
 * Created by lulei on 2018/2/26.
 */
public class SlidingTupleTsTopology {

    public static void main(String[] args) {
        TopologyBuilder builder =new TopologyBuilder();
        BaseWindowedBolt bolt = new SlidingWindowSumBolt().withWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS),new BaseWindowedBolt.Duration(3,TimeUnit.SECONDS))
                .withTimestampField("ts").withLag(new BaseWindowedBolt.Duration(5,TimeUnit.SECONDS));
        builder.setSpout("spout",new RandomIntegerSpout(),1);
        builder.setBolt("first",bolt,1).shuffleGrouping("spout");
        builder.setBolt("print",new PrinterBolt(),1).shuffleGrouping("first");

        Config config =new Config();
        config.setDebug(true);
        config.setNumWorkers(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mydemo",config,builder.createTopology());
    }
}
