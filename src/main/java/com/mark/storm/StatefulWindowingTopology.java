package com.mark.storm;

import com.mark.storm.bolt.PrinterBolt;
import com.mark.storm.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by lulei on 2018/2/26.
 */
public class StatefulWindowingTopology {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulWindowingTopology.class);

    public static class  WindowSumBolt extends BaseStatefulWindowedBolt<KeyValueState<String,Long>>{

        OutputCollector collector;
        private KeyValueState<String, Long> state;
        private long sum;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void initState(KeyValueState<String, Long> state) {
            this.state = state;
            sum = state.get("sum",0L);
            LOG.debug("initState with state [" + state + "] current sum [" + sum + "]");
        }

        @Override
        public void execute(TupleWindow tupleWindow) {
            for(Tuple tuple: tupleWindow.get()){
                sum += tuple.getIntegerByField("value");
            }
            state.put("sum",sum);
            collector.emit(new Values(sum));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sum"));
        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new RandomIntegerSpout());
        builder.setBolt("first",new WindowSumBolt().withWindow(new BaseWindowedBolt.Count(5),new BaseWindowedBolt.Count(3))
                .withMessageIdField("msgid"),1).shuffleGrouping("spout");
        builder.setBolt("second",new PrinterBolt(),1).shuffleGrouping("first");

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(3);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mydemo",config,builder.createTopology());


    }
}
