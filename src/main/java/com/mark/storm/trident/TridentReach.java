package com.mark.storm.trident;

import clojure.lang.Obj;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.daemon.drpc;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.ReadOnlyState;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.map.ReadOnlyMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;

import java.util.*;

/**
 * Created by lulei on 2018/2/28.
 */
public class TridentReach {
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

    public static class StaticSingleKeyMapState extends ReadOnlyState implements ReadOnlyMapState<Object>{
        public static class Factory implements StateFactory{
            Map map;

            public Factory(Map map) {
                this.map = map;
            }

            @Override
            public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i1) {
                return new StaticSingleKeyMapState(map);
            }
        }

        Map map;

        public StaticSingleKeyMapState(Map map) {
            this.map = map;
        }
        @Override
        public List<Object> multiGet(List<List<Object>> list) {
            List<Object> ret = new ArrayList();
            for(List<Object> key:list){
                Object singleKey = key.get(0);
                ret.add(map.get(singleKey));
            }
            return null;
        }
    }

    public static class  One implements CombinerAggregator<Integer>{

        @Override
        public Integer init(TridentTuple tridentTuple) {
            return 1;
        }

        @Override
        public Integer combine(Integer integer, Integer t1) {
            return 1;
        }

        @Override
        public Integer zero() {
            return 1;
        }
    }

    public static class  ExpandList extends BaseFunction{

        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            List  list = (List) tridentTuple.getValue(0);
            if(list != null){
                for(Object obj: list){
                    tridentCollector.emit(new Values(obj));
                }
            }
        }
    }

    public static StormTopology buildTopology(){
        TridentTopology topology = new TridentTopology();
        TridentState urlToTweeters = topology.newStaticState(new StaticSingleKeyMapState.Factory(TWEETERS_DB));
        TridentState tweetersToFollowers  = topology.newStaticState(new StaticSingleKeyMapState.Factory(FOLLOWERS_DB));

        topology.newDRPCStream("reach")
                .stateQuery(urlToTweeters,new Fields("args"),new MapGet(),new Fields("tweeters"))
                .each(new Fields("tweeters"),new ExpandList(),new Fields("tweeter"))
                .shuffle()
                .stateQuery(tweetersToFollowers,new Fields("tweeter"),new MapGet(),new Fields("followers"))
                .each(new Fields("followers"),new ExpandList(),new Fields("follower"))
                .groupBy(new Fields("follower"))
                .aggregate(new One(),new Fields("one"))
                .aggregate(new Fields("one"),new Sum(),new Fields("reach"));
        return topology.build();

    }

    public static void main(String[] args) throws TException, InterruptedException {
        Config config =new Config();
        config.setDebug(true);
        config.setNumWorkers(2);
        StormSubmitter.submitTopology("reach", config, buildTopology());

        DRPCClient drpcClient = new DRPCClient(config,"localhost",7000);
        Thread.sleep(2000);

        System.out.println("REACH: " + drpcClient.execute("reach", "aaa"));
        System.out.println("REACH: " + drpcClient.execute("reach", "foo.com/blog/1"));
        System.out.println("REACH: " + drpcClient.execute("reach", "engineering.twitter.com/blog/5"));

    }




}
