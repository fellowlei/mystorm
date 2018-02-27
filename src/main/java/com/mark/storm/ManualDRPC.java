package com.mark.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;

/**
 * Created by lulei on 2018/2/27.
 */
public class ManualDRPC {
    public static class ExclamationBolt extends BaseBasicBolt{
        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String arg = tuple.getString(0);
            Object retInfo = tuple.getValue(1);
            basicOutputCollector.emit(new Values(arg + "!!!",retInfo));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("result","return-info"));
        }
    }

    public static void main(String[] args) throws TException {
        TopologyBuilder builder =new TopologyBuilder();
        DRPCSpout spout =new DRPCSpout("demo");
        builder.setSpout("drpc",spout);
        builder.setBolt("first",new ExclamationBolt(),3).shuffleGrouping("drpc");
        builder.setBolt("second",new ReturnResults(),3).shuffleGrouping("first");

        Config config =new Config();
        config.setDebug(true);
        config.setNumWorkers(2);


        LocalCluster cluster =new LocalCluster();
        cluster.submitTopology("demo",config,builder.createTopology());
        DRPCClient drpcClient = new DRPCClient(config,"localhost",7777);
        System.out.println(drpcClient.execute("demo","aaa"));
        System.out.println(drpcClient.execute("demo","bbb"));
    }
}
