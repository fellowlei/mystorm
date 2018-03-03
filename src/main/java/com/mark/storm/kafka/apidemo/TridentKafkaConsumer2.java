package com.mark.storm.kafka.apidemo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.shade.org.apache.commons.collections.MapUtils;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class TridentKafkaConsumer2 {

    public static void main(String[] args) throws Exception {
        ZkHosts zkHosts = new ZkHosts("localhost:2181");
        TridentTopology topology = new TridentTopology();

        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(zkHosts, "my-topic","myclient");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());


        OpaqueTridentKafkaSpout opaqueTridentKafkaSpout = new OpaqueTridentKafkaSpout(
                kafkaConfig);
        topology.newStream("mystream", opaqueTridentKafkaSpout)
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple tridentTuple) {
                        System.out.println("###" + tridentTuple);
                    }
                });
//                .parallelismHint(3)
//                .shuffle()
//                .each(new Fields("str"), new SpilterFunction(), new Fields("sentence"))
//                .groupBy(new Fields("sentence"))
//                .aggregate(new Fields("sentence"), new SumWord(),
//                        new Fields("sum")).parallelismHint(5)
//                .each(new Fields("sum"), new PrintFilter_partition());
        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(2);
//        StormSubmitter.submitTopology("test_kafka2storm_opaqueTrident_topology", config,
//                topology.build());

        LocalCluster cluster =new LocalCluster();
        cluster.submitTopology("mydemo",config,topology.build());
    }

    private static class SpilterFunction extends BaseFunction {
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String sentence = tridentTuple.getString(0);
            String[] array = sentence.split("\\s+");
            for(int i=0; i<array.length; i++){
                System.out.println("spilter emit:" + array[i]);
                tridentCollector.emit(new Values(array[i]));
            }
        }
    }

    private static class SumWord extends BaseAggregator<Map<String,Integer>> {
        private Object batchId; // 属于哪个batch
        private int partitionId; // 属于哪个分区
        private int numPartitions;// 分区数量
        private Map<String,Integer> state; // 用来统计

        @Override
        public Map<String, Integer> init(Object batchId, TridentCollector tridentCollector) {
            this.batchId = batchId;
            return state;
        }

        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            state = new HashMap<>();
            partitionId = context.getPartitionIndex();
            numPartitions = context.numPartitions();
        }

        @Override
        public void aggregate(Map<String, Integer> val, TridentTuple tridentTuple, TridentCollector tridentCollector) {
            System.out.println(tridentTuple+";partitionId="+partitionId+";partitions="+numPartitions
                    +",batchId:" + batchId);
            String word = tridentTuple.getString(0);
            val.put(word, MapUtils.getIntValue(val,word,0) + 1);
            System.out.println("#sumWord:" + val);
        }

        @Override
        public void complete(Map<String, Integer> val, TridentCollector tridentCollector) {
            tridentCollector.emit(new Values(val));
        }
    }

    private static class PrintFilter_partition extends BaseFilter {
        @Override
        public boolean isKeep(TridentTuple tridentTuple) {
            System.out.println("##print tuple:" + tridentTuple);
            return false;
        }
    }
}
