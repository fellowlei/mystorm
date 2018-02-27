package com.mark.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.*;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lulei on 2018/2/27.
 */
public class InOrderDeliveryTest {
    public static class  InOrderSpout extends BaseRichSpout{
        SpoutOutputCollector spoutOutputCollector;
        int base = 0;
        int i=0;
        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
            this.base = topologyContext.getThisTaskIndex();
        }

        @Override
        public void nextTuple() {
            Values v =new Values(base,i);
            spoutOutputCollector.emit(v,"ACK");
            i++;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("c1","c2"));
        }
    }

    public static class Check extends BaseBasicBolt{
        Map<Integer, Integer> expected = new HashMap<Integer, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            Integer c1 = tuple.getInteger(0);
            Integer c2 = tuple.getInteger(1);
            Integer exp = expected.get(c1);
            if(exp == null) exp = 0;
            if (c2.intValue() != exp.intValue()) {
                System.out.println(c1+" "+c2+" != "+exp);
                throw new FailedException(c1+" "+c2+" != "+exp);
            }

            exp = c2 + 1;
            expected.put(c1,exp);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static void printMetrics(Nimbus.Iface client,String name) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        String id = null;
        for(TopologySummary ts: summary.get_topologies()){
            if(name.equals(ts.get_name())){
                id = ts.get_id();
            }
        }

        if(id == null){
            throw new Exception("Could not find a topology named "+name);
        }

        TopologyInfo info = client.getTopologyInfo(id);
        int uptime = info.get_uptime_secs();
        long acked = 0;
        long failed = 0;
        double weightedAvgTotal = 0.0;

        for(ExecutorSummary exec: info.get_executors()){
            if("spout".equals(exec.get_component_id())){
                SpoutStats stats = exec.get_stats().get_specific().get_spout();
                Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                Map<String, Double> avgLatMap  = stats.get_complete_ms_avg().get(":all-time");
                for(String key: ackedMap.keySet()){
                    if(failedMap != null){
                        Long tmp = failedMap.get(key);
                        if(tmp != null){
                            failed += tmp;
                        }
                    }
                    long ackVal = ackedMap.get(key);
                    double latVal = avgLatMap.get(key) * ackVal;
                    acked += ackVal;
                    weightedAvgTotal +=latVal;
                }
            }
        }

        double avgLatency = weightedAvgTotal/acked;
        System.out.println("uptime: "+uptime+" acked: "+acked+" avgLatency: "+avgLatency+" acked/sec: "+(((double)acked)/uptime+" failed: "+failed));
    }

    public static void kill(Nimbus.Iface client,String name) throws TException {
        KillOptions options = new KillOptions();
        options.set_wait_secs(0);
        client.killTopologyWithOpts(name,options);
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new InOrderSpout(),8);
        builder.setBolt("first",new Check(),8).fieldsGrouping("spout",new Fields("c1"));

        Config config =new Config();
        config.registerMetricsConsumer(LoggingMetricsConsumer.class);
        config.setDebug(true);
        config.setNumWorkers(1);
        String name = "mydemo";

        LocalCluster cluster =new LocalCluster();
        cluster.submitTopology(name,config,builder.createTopology());

        Map<String, Object> clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
        //Sleep for 50 mins
        for (int i = 0; i < 50; i++) {
            Thread.sleep(30 * 1000);
            printMetrics(client, name);
        }
        kill(client, name);

    }
}
