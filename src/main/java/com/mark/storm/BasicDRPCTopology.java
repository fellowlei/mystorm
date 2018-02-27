package com.mark.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by lulei on 2018/2/27.
 */
public class BasicDRPCTopology {

    public static class  ExclaimBolt extends BaseBasicBolt{

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String input = tuple.getString(1);
            basicOutputCollector.emit(new Values(tuple.getValue(0),input + "!!!"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","result"));
        }
    }

    public static void main(String[] args) {
    }
}
