package com.mark.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.thrift.TException;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lulei on 2018/2/28.
 */
public class TridentMapExample {
    private static MapFunction toUpper = new MapFunction() {
        @Override
        public Values execute(TridentTuple tridentTuple) {
            return new Values(tridentTuple.getStringByField("word").toUpperCase());
        }
    };

    private static FlatMapFunction split = new FlatMapFunction() {
        @Override
        public Iterable<Values> execute(TridentTuple tridentTuple) {
            List<Values> valuesList =new ArrayList<>();
            for(String word:tridentTuple.getString(0).split(" ")){
                valuesList.add(new Values(word));
            }
            return valuesList;
        }
    };


    private static Filter theFilter = new BaseFilter() {
        @Override
        public boolean isKeep(TridentTuple tridentTuple) {
            return tridentTuple.getString(0).equals("THE");
        }
    };

    public static StormTopology buildTopology(){
        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("word"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology =new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1",spout).parallelismHint(16)
                .flatMap(split)
                .map(toUpper,new Fields("uppercased"))
                .filter(theFilter)
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple tridentTuple) {
                        System.out.println(tridentTuple.getString(0));
                    }
                })
                .groupBy(new Fields("uppercased"))
                .persistentAggregate(new MemoryMapState.Factory(),new Count(),new Fields("count"))
                .parallelismHint(16);

        topology.newDRPCStream("words")
                .flatMap(split,new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts,new Fields("word"),new MapGet(),new Fields("count"))
                .filter(new FilterNull())
                .aggregate(new Fields("count"),new Sum(),new Fields("sum"));
        return topology.build();

    }

    public static void main(String[] args) throws TException, InterruptedException {
        Config config =new Config();
        config.setMaxSpoutPending(20);
        config.setDebug(true);
        config.setNumWorkers(2);
        StormSubmitter.submitTopology("wordCount", config, buildTopology());

        DRPCClient drpcClient = new DRPCClient(config,"localhost",7000);
        for (int i = 0; i < 10; i++) {
            System.out.println("DRPC RESULT: " + drpcClient.execute("words", "CAT THE DOG JUMPED"));
            Thread.sleep(1000);
        }

    }



}
