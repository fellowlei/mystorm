package com.mark.storm.kafka.demo;

import com.mark.storm.bolt.PrinterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by lulei on 2018/3/2.
 */
public class BoltKafkaConsumer {

    public static void main(String[] args) {
        final TopologyBuilder builder = new TopologyBuilder();

        //配置zookeeper 主机:端口号
        BrokerHosts brokerHosts =new ZkHosts("localhost:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "my-topic", "", "mydemo");
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("kafka_spout", kafkaSpout, 1);
        builder.setBolt("bolt", new PrinterBolt()).shuffleGrouping("kafka_spout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafkaConsumerTest",conf,builder.createTopology());
    }
}
