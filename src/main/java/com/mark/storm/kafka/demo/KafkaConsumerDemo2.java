package com.mark.storm.kafka.demo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerDemo2 {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        // BrokerHosts接口有2个实现类StaticHosts和ZkHosts,ZkHosts会定时(默认60秒)从ZK中更新brokers的信息,StaticHosts是则不会
        // 要注意这里的第二个参数brokerZkPath要和kafka中的server.properties中配置的zookeeper.connect对应
        // 因为这里是需要在zookeeper中找到brokers znode
        // 默认kafka的brokers znode是存储在zookeeper根目录下
        BrokerHosts brokerHosts = new ZkHosts("localhost:2181");

        // 定义spoutConfig
        // 第一个参数hosts是上面定义的brokerHosts
        // 第二个参数topic是该Spout订阅的topic名称
        // 第三个参数zkRoot是存储消费的offset(存储在ZK中了),当该topology故障重启后会将故障期间未消费的message继续消费而不会丢失(可配置)
        // 第四个参数id是当前spout的唯一标识
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "my-topic", "", "myid");

        // 定义kafkaSpout如何解析数据,这里是将kafka的producer send的数据放入到String
        // 类型的str变量中输出,这个str是StringSchema定义的变量名称
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        // 设置spout
        builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig));
        // 设置bolt
        builder.setBolt("splitBolt", new WordSplitBolt()).shuffleGrouping("kafkaSpout");
        // 设置bolt
        builder.setBolt("countBolt", new WordCountBolt()).fieldsGrouping("splitBolt", new Fields("word"));

        // 本地运行
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mydemo",config,builder.createTopology());

    }

    private static class WordSplitBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            // 根据变量名获得从spout传来的值,这里的str是spout中定义好的变量名
            String line = tuple.getStringByField("str");

            // 对单词进行分割
            for (String word : line.split(" ")) {
                // 传递给下一个组件，即WordCountBolt
                basicOutputCollector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    private static class WordCountBolt extends BaseBasicBolt {

        private static Map<String, Integer> map = new HashMap<String, Integer>();
        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            // 根据变量名称获得上一个bolt传递过来的数据
            String word = tuple.getStringByField("word");

            Integer count = map.get(word);
            if (count == null) {
                map.put(word, 1);
            } else {
                count ++;
                map.put(word, count);
            }

            StringBuilder msg = new StringBuilder();
            for(Map.Entry<String, Integer> entry : map.entrySet()){
                msg.append(entry.getKey() + " = " + entry.getValue()).append(", ");
            }

            System.out.println("###" + msg.toString());
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }
}
