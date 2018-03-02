package com.mark.storm.kafka.demo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by lulei on 2018/3/1.
 */
public class BoltKafkaDemo {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();

//        Fields fields = new Fields("key", "message");
//        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
//                new Values("storm", "1"),
//                new Values("trident", "1"),
//                new Values("needs", "1"),
//                new Values("javadoc", "1")
//        );
//        spout.setCycle(true);
        builder.setSpout("spout", new UUIDSpout(), 5);
//set producer properties.
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaBolt bolt = new KafkaBolt()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("my-topic"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key","message"));
        builder.setBolt("forwardToKafka", bolt, 8).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafkaboltTest",conf,builder.createTopology());
    }


    public static class UUIDSpout extends BaseRichSpout {
        SpoutOutputCollector spoutOutputCollector;
        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(1000);
            String value = UUID.randomUUID().toString();
            spoutOutputCollector.emit(new Values(value,value));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("key","message"));
        }
    }

}
