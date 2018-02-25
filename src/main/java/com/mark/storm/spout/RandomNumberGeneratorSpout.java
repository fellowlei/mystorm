package com.mark.storm.spout;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Administrator on 2018/2/25.
 */
public class RandomNumberGeneratorSpout implements IBatchSpout {
    private final Fields fields;
    private final int batchSize;
    private final int maxNumber;
    private final Map<Long, List<List<Object>>> batches = new HashMap<>();

    public RandomNumberGeneratorSpout(Fields fields, int batchSize, int maxNumber) {
        this.fields = fields;
        this.batchSize = batchSize;
        this.maxNumber = maxNumber;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void emitBatch(long batchId, TridentCollector tridentCollector) {
        List<List<Object>> values = null;
        if (batches.containsKey(batchId)) {
            values = batches.get(batchId);
        } else {
            values = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                List<Object> numbers = new ArrayList<>();
                for (int x = 0; x < fields.size(); i++) {
                    numbers.add(ThreadLocalRandom.current().nextInt(0, maxNumber + 1));
                }
                values.add(numbers);
            }
            batches.put(batchId, values);
        }

        for (List<Object> value : values) {
            tridentCollector.emit(value);
        }
    }

    @Override
    public void ack(long batchId) {
        batches.remove(batchId);
    }

    @Override
    public void close() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config = new Config();
        config.setMaxTaskParallelism(1);
        return config;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }
}
