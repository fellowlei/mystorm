package com.mark.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2018/2/25.
 */
public class WordCountTopology {

    public static class SplitSententBolt  extends BaseRichBolt {
        OutputCollector outputCollector;
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        public void execute(Tuple tuple) {
            String msg = tuple.getString(0);
            String[] words = msg.split(" ");
            for(String word:words){
                outputCollector.emit(new Values(word));
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }
    public static class WordCountBolt extends BaseBasicBolt{
        Map<String,Integer> counts = new HashMap<String, Integer>();

        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if(count == null)
                count = 0;
            count++;
            counts.put(word,count);
            System.out.println(counts);
            basicOutputCollector.emit(new Values(word,count));
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word","count"));
        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new RandomSentenceSpout(),2);
        builder.setBolt("split",new SplitSententBolt(),2).shuffleGrouping("spout");
        builder.setBolt("count",new WordCountBolt(),2).fieldsGrouping("split",new Fields("word"));

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("myWordCount",config,builder.createTopology());

    }
}
