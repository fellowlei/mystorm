package com.mark.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Created by Administrator on 2018/2/25.
 */
public class ExclamationTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word",new TestWordSpout(),1);
        builder.setBolt("first",new ExclamationBolt(),1).shuffleGrouping("word");
        builder.setBolt("second",new ExclamationBolt(),1).shuffleGrouping("first");

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(3);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("mystorm",config,builder.createTopology());
        Utils.sleep(1000 * 5);
        localCluster.killTopology("mystorm");
        localCluster.shutdown();

    }


    public static class ExclamationBolt extends BaseRichBolt{

        OutputCollector _collector;
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            _collector.emit(tuple,new Values(tuple.getString(0) + "!!!"));
            _collector.ack(tuple);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }
}
