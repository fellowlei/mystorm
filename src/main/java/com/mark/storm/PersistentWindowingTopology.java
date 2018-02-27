package com.mark.storm;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by lulei on 2018/2/27.
 */
public class PersistentWindowingTopology {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentWindowingTopology.class);
    // wrapper to hold global and window averages
    private static class Averages {
        private final double global;
        private final double window;

        Averages(double global, double window) {
            this.global = global;
            this.window = window;
        }

        @Override
        public String toString() {
            return "Averages{" + "global=" + String.format("%.2f", global) + ", window=" + String.format("%.2f", window) + '}';
        }
    }


    private static class AvgBolt extends BaseStatefulWindowedBolt<KeyValueState<String,Long>>{

        KeyValueState<String, Long> state;
        @Override
        public void initState(KeyValueState<String, Long> state) {
            this.state = state;
        }

        @Override
        public void execute(TupleWindow tupleWindow) {
            int sum = 0;
            int count = 0;
            List<Tuple> tuples = tupleWindow.get();
            for(Tuple tuple: tuples){
                sum += tuple.getInteger(0);
                ++count;
            }
            LOG.debug("Count : {}", count);

        }
    }


}
