package com.mark.storm.kafka.apidemo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Created by lulei on 2018/3/2.
 */
public class TridentKafkaConsumer {
    public static void main(String[] args) {
        final TridentTopology tridentTopology = new TridentTopology();
        //配置zookeeper 主机:端口号
        BrokerHosts brokerHosts =new ZkHosts("localhost:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "my-topic", "", "mydemo");
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        final Stream spoutStream = tridentTopology.newStream("kafkaSpout", kafkaSpout).parallelismHint(1).toStream();
        spoutStream.peek(new Consumer() {
            @Override
            public void accept(TridentTuple tridentTuple) {
                System.out.println("####" + tridentTuple);
            }
        });
        Config conf = new Config();
        conf.setMaxSpoutPending(1);
        conf.setNumWorkers(1);
        conf.setDebug(true);

        LocalCluster cluster =new LocalCluster();
        cluster.submitTopology("mydemo",conf,tridentTopology.build());

    }
}
