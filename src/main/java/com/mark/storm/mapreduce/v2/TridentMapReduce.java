package com.mark.storm.mapreduce.v2;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TridentMapReduce {
    public static void main(String[] args) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 1, new Values("spark hadoop"), new Values("hadoop hive"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(1)
                .each(new Fields("sentence"), new SplitFunction(), new Fields("word"))
                .groupBy(new Fields("word")).
                        persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(1);

        // debug value stream
        wordCounts.newValuesStream().peek(new Consumer() {
            @Override
            public void accept(TridentTuple tridentTuple) {
                System.out.println("###" + tridentTuple);
            }
        });



        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(1);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("mydemo",config,topology.build());
    }
    // split function
    static class SplitFunction extends BaseFunction{

        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String sentence = tridentTuple.getString(0);
            String[] words = sentence.split(" ");
            for(String word: words){
                tridentCollector.emit(new Values(word));
            }
        }
    }
}
