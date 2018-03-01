package com.mark.storm.kafka.bolt;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by lulei on 2018/3/1.
 */
public class KafkaProducerTopology {

    public static StormTopology newTopology(String brokerUrl, String topicName){
        TopologyBuilder builder  = new TopologyBuilder();
        builder.setSpout("spout",new MyKafkaSpout());

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, topicName);


        KafkaBolt<String,String> bolt = new KafkaBolt<String,String>()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector(topicName))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key","value"));

        builder.setBolt("tokafka",bolt,1).shuffleGrouping("spout");
        return builder.createTopology();
    }

    public static void main(String[] args) {
        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mydemo",config,newTopology("localhost:9092","my-topic"));

        Utils.sleep(5000);
    }

    public static class MyKafkaSpout extends BaseRichSpout{
        SpoutOutputCollector spoutOutputCollector;
        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
        }

        @Override
        public void nextTuple() {
            Utils.sleep(1000);
            spoutOutputCollector.emit(new Values(UUID.randomUUID().toString()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("value"));
        }
    }
}
