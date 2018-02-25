package com.mark.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;

import java.util.*;

/**
 * Created by Administrator on 2018/2/25.
 */
public class ReachTopology {

    public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
        put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
        put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
        put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
    }};

    public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
        put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
        put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
        put("tim", Arrays.asList("alex"));
        put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
        put("adam", Arrays.asList("david", "carissa"));
        put("mike", Arrays.asList("john", "bob"));
        put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
    }};


    public static class GetTweeters extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            Object id = tuple.getValue(0);
            String url = tuple.getString(1);
            List<String> tweeters  = TWEETERS_DB.get(url);
            if(tweeters != null){
                for(String tweeter: tweeters){
                    basicOutputCollector.emit(new Values(id,tweeter));
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","tweeter"));
        }
    }

    public static class GetFollowers extends BaseBasicBolt{

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            Object id = tuple.getValue(0);
            String tweeter  = tuple.getString(1);
            List<String> followers  = FOLLOWERS_DB.get(tweeter);
            if(followers != null){
                for(String follower: followers){
                    basicOutputCollector.emit(new Values(id,follower));
                }
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","follower"));
        }
    }

    public static class PartialUniquer extends BaseBatchBolt<Object>{
        BatchOutputCollector batchOutputCollector;
        Object id;

        Set<String> followers = new HashSet<String>();
        @Override
        public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, Object id) {
            this.batchOutputCollector = batchOutputCollector;
            this.id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            followers.add(tuple.getString(1));
        }

        @Override
        public void finishBatch() {
            batchOutputCollector.emit(new Values(id,followers.size()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","count"));
        }
    }

    public static class CountAggregator extends BaseBatchBolt<Object>{

        BatchOutputCollector batchOutputCollector;
        Object id;
        int count =0;
        @Override
        public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, Object id) {
            this.batchOutputCollector = batchOutputCollector;
            this.id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            count += tuple.getInteger(1);
        }

        @Override
        public void finishBatch() {
            batchOutputCollector.emit(new Values(id,count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","reach"));
        }
    }

    public static void main(String[] args) throws TException {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
        builder.addBolt(new GetTweeters(), 4);
        builder.addBolt(new GetFollowers(), 12).shuffleGrouping();
        builder.addBolt(new PartialUniquer(), 6).fieldsGrouping(new Fields("id", "follower"));
        builder.addBolt(new CountAggregator(), 3).fieldsGrouping(new Fields("id"));

        Config config = new Config();
        config.setNumWorkers(4);
        config.setDebug(true);

        StormSubmitter.submitTopologyWithProgressBar("mydemo",config,builder.createRemoteTopology());


        DRPCClient drpcClient = new DRPCClient(config, "localhost", 6001);
        String[] urlsToTry = new String[]{ "foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com" };
        for (String url : urlsToTry) {
            System.out.println("Reach of " + url + ": " + drpcClient.execute("reach", url));
        }
    }


}
