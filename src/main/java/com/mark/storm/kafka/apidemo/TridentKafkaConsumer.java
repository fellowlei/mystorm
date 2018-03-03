package com.mark.storm.kafka.apidemo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Created by lulei on 2018/3/2.
 */
public class TridentKafkaConsumer {
    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        //配置zookeeper 主机:端口号
        BrokerHosts brokerHosts =new ZkHosts("localhost:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "my-topic", "", "mydemo2");
        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(brokerHosts, "my-topic", "spout");
//        tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new ConvertStringScheme());
        /**
         * 支持事物,支持失败重发
         *
         */

        TransactionalTridentKafkaSpout transactionalTridentKafkaSpout = new TransactionalTridentKafkaSpout(
                tridentKafkaConfig);
//        topology.newStream("name",transactionalTridentKafkaSpout)
//                .shuffle()
//                .each(new Fields("msg"), new SpilterFunction(), new Fields("sentence"))
//                .groupBy(new Fields("sentence"))
//                .aggregate(new Fields("sentence"), new SumWord(),new Fields("sum"))
//                .parallelismHint(5)
//                .each(new Fields("sum"), new PrintFilter_partition());
        topology.newStream("mystream",transactionalTridentKafkaSpout).peek(new Consumer() {
            @Override
            public void accept(TridentTuple tridentTuple) {
                System.out.println("####" + tridentTuple);
            }
        });
        Config config = new Config();
        config.setMaxSpoutPending(1);
        config.setNumWorkers(1);
        config.setDebug(false);

        LocalCluster cluster =new LocalCluster();
        cluster.submitTopology("mydemo",config,topology.build());
    }

}
