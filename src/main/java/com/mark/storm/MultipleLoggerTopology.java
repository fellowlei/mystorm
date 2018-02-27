package com.mark.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by lulei on 2018/2/27.
 */
public class MultipleLoggerTopology {
    public static class ExclamationLoggingBolt extends BaseRichBolt{
        OutputCollector outputCollector;
        Logger _rootLogger = LoggerFactory.getLogger (Logger.ROOT_LOGGER_NAME);
        Logger _logger = LoggerFactory.getLogger ("com.myapp");
        Logger _subLogger = LoggerFactory.getLogger ("com.myapp.sub");

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            _rootLogger.debug ("root: This is a DEBUG message");
            _rootLogger.info ("root: This is an INFO message");
            _rootLogger.warn ("root: This is a WARN message");
            _rootLogger.error ("root: This is an ERROR message");

            _logger.debug ("myapp: This is a DEBUG message");
            _logger.info ("myapp: This is an INFO message");
            _logger.warn ("myapp: This is a WARN message");
            _logger.error ("myapp: This is an ERROR message");

            _subLogger.debug ("myapp.sub: This is a DEBUG message");
            _subLogger.info ("myapp.sub: This is an INFO message");
            _subLogger.warn ("myapp.sub: This is a WARN message");
            _subLogger.error ("myapp.sub: This is an ERROR message");

            outputCollector.emit(tuple,new Values(tuple.getString(0) + "!!!"));
            outputCollector.ack(tuple);

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder =new TopologyBuilder();
        builder.setSpout("spout",new TestWordSpout(),2);
        builder.setBolt("first",new ExclamationLoggingBolt(),3).shuffleGrouping("spout");
        builder.setBolt("second",new ExclamationLoggingBolt(),2).shuffleGrouping("first");

        Config config =new Config();
        config.setDebug(true);
        config.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mydemo",config,builder.createTopology());
    }
}
