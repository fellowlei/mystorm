package com.mark.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by lulei on 2018/2/26.
 */
public class ResourceAwareExampleTopology {

    public static class ExclamationBolt extends BaseRichBolt{
        private static final ConcurrentHashMap<String, String> myCache = new ConcurrentHashMap<>();
        private static final int CACHE_SIZE = 100_000;
        OutputCollector outputCollector;
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        static String getFromCache(String key) {
            return myCache.get(key);
        }

        static void addToCache(String key,String value){
            myCache.putIfAbsent(key, value);
            int numToRemove = myCache.size() - CACHE_SIZE;
            if(numToRemove > 0){
                Iterator<Map.Entry<String, String>> it = myCache.entrySet().iterator();
                for(; numToRemove > 0 && it.hasNext(); numToRemove--){
                    it.next();
                    it.remove();
                }
            }
        }

        @Override
        public void execute(Tuple tuple) {
            String orig = tuple.getString(0);
            String ret = getFromCache(orig);
            if(ret == null){
                ret = orig + "!!!";
                addToCache(orig,ret);
            }
            outputCollector.emit(tuple,new Values(ret));
            outputCollector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder =new TopologyBuilder();
        SpoutDeclarer spout = builder.setSpout("spout",new TestWordSpout(),10).setCPULoad(20);
        spout.setMemoryLoad(64,16);

        builder.setBolt("first",new ExclamationBolt(),3).shuffleGrouping("spout");

        builder.setBolt("second",new ExclamationBolt(),2).shuffleGrouping("first")
                .setMemoryLoad(100);

        Config conf = new Config();
        conf.setDebug(true);
        conf.setTopologyWorkerMaxHeapSize(1024.0);
        conf.setTopologyPriority(29);
        conf.setTopologyStrategy(DefaultResourceAwareStrategy.class);

        StormSubmitter.submitTopology("mydemo",conf,builder.createTopology());


    }
}
