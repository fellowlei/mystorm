package com.mark.storm.kafka.demo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lulei on 2018/3/2.
 */
public class KafkaDemo1 {
    public static void main(String[] args) throws Exception {
        //配置zookeeper 主机:端口号
        BrokerHosts brokerHosts =new ZkHosts("localhost:2181");
        //接收消息队列的主题
        String topic="recommend";
        //zookeeper设置文件中的配置，如果zookeeper配置文件中设置为主机名：端口号 ，该项为空
        String zkRoot="";
        //任意
        String spoutId="zhou";
        SpoutConfig spoutConfig=new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
        //设置如何处理kafka消息队列输入流
        spoutConfig.scheme=new SchemeAsMultiScheme(new MessageScheme());
        Config conf=new Config();
        //不输出调试信息
        conf.setDebug(false);
        //设置一个spout task中处于pending状态的最大的tuples数量
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        Map<String, String> map=new HashMap<String,String>();
        // 配置Kafka broker地址
        map.put("metadata.broker.list", "master:9092,slave1:9092,slave2:9092");
        // serializer.class为消息的序列化类
        map.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put("kafka.broker.properties", map);
        // 配置KafkaBolt生成的topic
        conf.put("topic", "receiver");
        TopologyBuilder builder =new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(spoutConfig),1);
        builder.setBolt("bolt1", new QueryBolt(),1).setNumTasks(1).shuffleGrouping("spout");
        builder.setBolt("bolt2", new KafkaBolt<String, String>(),1).setNumTasks(1).shuffleGrouping("bolt1");
        if(args.length==0){
            LocalCluster cluster = new LocalCluster();
            //提交本地集群
            cluster.submitTopology("test", conf, builder.createTopology());
            //等待6s之后关闭集群
            Thread.sleep(6000);
            //关闭集群
            cluster.shutdown();
        }
        StormSubmitter.submitTopology("test", conf, builder.createTopology());
    }

    public static class  MessageScheme implements Scheme{
        private static final Logger LOGGER = LoggerFactory.getLogger(MessageScheme.class);

        @Override
        public List<Object> deserialize(ByteBuffer byteBuffer) {
            //从kafka中读取的值直接序列化为UTF-8的str
            String mString= null;
            try {
                mString = new String(byteBuffer.array(), "UTF-8");
                return new Values(mString);
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("Cannot parse the provided message");
            }

            return null;
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("msg");
        }
    }

    public static class  QueryBolt implements IRichBolt{

        List<String> list;
        OutputCollector outputCollector;
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            list=new ArrayList<String>();
            this.outputCollector=outputCollector;
        }

        @Override
        public void execute(Tuple input) {
            // TODO Auto-generated method stub
            String str=(String) input.getValue(0);
            //将str加入到list
            list.add(str);
            //发送ack
            outputCollector.ack(input);
            //发送该str
            outputCollector.emit(new Values(str));
        }

        @Override
        public void cleanup() { //topology被killed时调用
            //将list的值写入到文件
            try {
                FileOutputStream outputStream=new FileOutputStream("d://"+this+".txt");
                PrintStream p=new PrintStream(outputStream);
                p.println("begin!");
                p.println(list.size());
                for(String tmp:list){
                    p.println(tmp);
                }
                p.println("end!");
                try {
                    p.close();
                    outputStream.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("message"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
}
