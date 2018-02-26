package com.mark.storm;

import com.mark.storm.spout.RandomIntegerSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by lulei on 2018/2/26.
 */
public class StatefulTopology {
    private static final Logger LOG = LoggerFactory.getLogger(StatefulTopology.class);

    public static class StatefulSumBolt extends BaseStatefulBolt<KeyValueState<String,Long>>{

        OutputCollector collector;
        long sum;
        String name;
        KeyValueState<String,Long> kvState;

        public StatefulSumBolt(String name) {
            this.name = name;
        }

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            Object value = tuple.getValueByField("value");
            LOG.debug("{} sum = {}", name, sum);
            sum += ((Number)value).longValue();
            kvState.put("sum",sum);
            collector.emit(tuple,new Values(sum));
            collector.ack(tuple);
        }

        @Override
        public void initState(KeyValueState<String, Long> state) {
            kvState = state;
            sum = kvState.get("sum",0L);
            LOG.debug("Initstate, sum from saved state = {} ", sum);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("value"));
        }
    }

    public static class PrinterBolt extends BaseBasicBolt{

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            System.out.println("###" + tuple);
            LOG.debug("Got tuple {}", tuple);
            basicOutputCollector.emit(tuple.getValues());

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("value"));
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout",new RandomIntegerSpout());
        builder.setBolt("first",new StatefulSumBolt("first"),1).shuffleGrouping("spout");
        builder.setBolt("second",new PrinterBolt(),1).shuffleGrouping("first");
        builder.setBolt("total",new StatefulSumBolt("total"),1).shuffleGrouping("second");

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(4);


        LocalCluster cluster =new LocalCluster();
        cluster.submitTopology("mytopo",config,builder.createTopology());
    }
}
