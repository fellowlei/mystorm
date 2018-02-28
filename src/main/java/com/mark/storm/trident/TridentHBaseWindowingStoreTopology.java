package com.mark.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.TumblingCountWindow;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lulei on 2018/2/28.
 */
public class TridentHBaseWindowingStoreTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TridentHBaseWindowingStoreTopology.class);

    public static StormTopology buildTopology(WindowsStoreFactory windowsStoreFactory){
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);


        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1",spout).parallelismHint(16)
                .each(new Fields("sentence"),new Split(),new Fields("word"))
                .window(TumblingCountWindow.of(1000),windowsStoreFactory,new Fields("word"),new CountAsAggregator(),new Fields("count"))
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple tridentTuple) {
                        LOG.info("Received tuple: [{}]", tridentTuple);
                    }
                });
        return topology.build();

    }

    public static void main(String[] args) {
        Config config = new Config();
        config.setMaxSpoutPending(20);
        config.put(Config.TOPOLOGY_TRIDENT_WINDOWING_INMEMORY_CACHE_LIMIT,100);

        config.setNumWorkers(3);
//        StormSubmitter.submitTopologyWithProgressBar("mydemo", config, buildTopology(windowStoreFactory));

    }
}
