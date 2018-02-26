package com.mark.storm;

import com.mark.storm.bolt.PrinterBolt;
import com.mark.storm.bolt.SlidingWindowSumBolt;
import com.mark.storm.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by lulei on 2018/2/26.
 */
public class SlidingWindowTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowTopology.class);

    private static class TumblingWindowAvgBolt extends BaseWindowedBolt{

        OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
           this.collector = collector;
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            int sum = 0;
            List<Tuple> tuplesInWindows = inputWindow.get();
            if(tuplesInWindows.size() > 0){
                for(Tuple tuple: tuplesInWindows){
                    sum += (int)tuple.getValue(0);
                }
            }
            collector.emit(new Values(sum / tuplesInWindows.size()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("avg"));
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new RandomIntegerSpout(),1);
        // todo set fist bolt

        builder.setBolt("first",new SlidingWindowSumBolt().withWindow(BaseWindowedBolt.Count.of(30), BaseWindowedBolt.Count.of(10)),1)
                .shuffleGrouping("spout");
        builder.setBolt("second",new TumblingWindowAvgBolt().withTumblingWindow(BaseWindowedBolt.Count.of(3)),1).shuffleGrouping("first");
        builder.setBolt("print",new PrinterBolt(),1).shuffleGrouping("second");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(4);

        LocalCluster cluster =new LocalCluster();
        cluster.submitTopology("mydemo",conf,builder.createTopology());
    }
}
