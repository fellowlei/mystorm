package com.mark.storm.trident;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.state.CombinerValueUpdater;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.ValueUpdater;
import org.apache.storm.trident.testing.MemoryMapState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by lulei on 2018/2/28.
 */
public class DebugMemoryMapState<T> extends MemoryMapState<T> {
    private static final Logger LOG = LoggerFactory.getLogger(DebugMemoryMapState.class);

    private int updateCount = 0;
    public DebugMemoryMapState(String id) {
        super(id);
    }

    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        print(keys,updaters);
        if((updateCount++ % 5) == 0){
            LOG.error("Throwing FailedException");
            throw new FailedException("Enforced State Update Fail. On retrial should replay the exact same batch.");
        }
        return super.multiUpdate(keys, updaters);
    }

    private void print(List<List<Object>> keys, List<ValueUpdater> updaters) {
        for(int i=0; i<keys.size(); i++){
            ValueUpdater valueUpdater = updaters.get(i);
            Object arg = ((CombinerValueUpdater)valueUpdater).getArg();
            LOG.info("updateCount = {}, keys = {} => updaterArgs = {}", updateCount, keys.get(i), arg);
        }
    }

    public static class Factory implements StateFactory{
        String _id;

        public Factory() {
            _id = UUID.randomUUID().toString();
        }

        @Override
        public State makeState(Map map, IMetricsContext iMetricsContext, int partitionIndex, int numPartitions) {
            return new DebugMemoryMapState(_id + partitionIndex);
        }
    }


}
