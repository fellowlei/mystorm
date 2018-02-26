package com.mark.storm;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.MemoryTransactionalSpout;
import org.apache.storm.topology.IComponent;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.transactional.TransactionalTopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by lulei on 2018/2/26.
 */
public class TransactionalGlobalCount {
    public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {{
        put(0, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("chicken"));
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("apple"));
        }});
        put(1, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("apple"));
            add(new Values("banana"));
        }});
        put(2, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("dog"));
            add(new Values("dog"));
            add(new Values("dog"));
        }});
    }};

    public static class Value {
        int count = 0;
        BigInteger txid;
    }

    public static Map<String, Value> DATABASE = new HashMap<String, Value>();
    public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";

    public static class BatchCount extends BaseBatchBolt<Object>{
        BatchOutputCollector batchOutputCollector;
        Object id;
        int count = 0;
        @Override
        public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, Object id) {
            this.batchOutputCollector= batchOutputCollector;
            this.id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            count++;
        }

        @Override
        public void finishBatch() {
            batchOutputCollector.emit(new Values(id,count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","count"));
        }
    }

    public static class  UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {

        BatchOutputCollector batchOutputCollector;
        TransactionAttempt transactionAttempt;
        int sum =0;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, BatchOutputCollector batchOutputCollector, TransactionAttempt transactionAttempt) {
            this.batchOutputCollector = batchOutputCollector;
            this.transactionAttempt = transactionAttempt;
        }

        @Override
        public void execute(Tuple tuple) {
            sum += tuple.getInteger(1);
        }

        @Override
        public void finishBatch() {
            Value val  = DATABASE.get(GLOBAL_COUNT_KEY);
            Value newVal;

            if(val == null || !val.txid.equals(transactionAttempt.getTransactionId())){
                newVal = new Value();
                newVal.txid = transactionAttempt.getTransactionId();
                if (val == null) {
                    newVal.count = sum;
                }else{
                    newVal.count = sum + val.count;
                }
                DATABASE.put(GLOBAL_COUNT_KEY,newVal);
            }else{
                newVal = val;
            }
            batchOutputCollector.emit(new Values(transactionAttempt,newVal.count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","sum"));
        }
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA,new Fields("word"),3);
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 3);
        builder.setBolt("first",new BatchCount(),2).noneGrouping("spout");
        builder.setBolt("sum",new UpdateGlobalCount()).globalGrouping("first");
        Config config = new Config();
        config.setDebug(true);
        config.setMaxSpoutPending(3);

        StormSubmitter.submitTopology("mydemo",config,builder.buildTopology());
    }
}
