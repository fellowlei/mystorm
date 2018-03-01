package com.mark.storm.kafka.trident;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lulei on 2018/3/1.
 */
public class TridentKafkaConsumerTopology {
    protected static final Logger LOG = LoggerFactory.getLogger(TridentKafkaConsumerTopology.class);

    public static StormTopology newTopology(ITridentDataSource tridentSpout){
        TridentTopology tridentTopology = new TridentTopology();
        Stream spoutStream = tridentTopology.newStream("spout", tridentSpout).parallelismHint(2);
        spoutStream.each(spoutStream.getOutputFields(),new Debug(false));
        return tridentTopology.build();

    }
}
