package com.mark.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by lulei on 2018/2/28.
 */
public class TridentMinMaxOfVehiclesTopology {

    static class SpeedComparator implements Comparator<TridentTuple>,Serializable{

        @Override
        public int compare(TridentTuple tuple1, TridentTuple tuple2) {
            Vehicle vehicle1 = (Vehicle) tuple1.getValueByField(Vehicle.FIELD_NAME);
            Vehicle vehicle2 = (Vehicle) tuple2.getValueByField(Vehicle.FIELD_NAME);
            return Integer.compare(vehicle1.maxSpeed,vehicle2.maxSpeed);
        }
    }

    static class EfficiencyComparator implements Comparator<Vehicle>,Serializable{

        @Override
        public int compare(Vehicle vehicle1, Vehicle vehicle2) {
            return Double.compare(vehicle1.efficiency,vehicle2.efficiency);
        }

    }
    public static StormTopology buildVehiclesTopology(){
        Fields driverField = new Fields(Driver.FIELD_NAME);
        Fields vehicleField = new Fields(Vehicle.FIELD_NAME);
        Fields allFields = new Fields(Vehicle.FIELD_NAME, Driver.FIELD_NAME);

        FixedBatchSpout spout = new FixedBatchSpout(allFields,10,Vehicle.generateVehicles(20));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream vehiclesStream = topology.newStream("spout1",spout)
                .each(allFields,new Debug("#### vehicles"));

        Stream slowVehiclesStream = vehiclesStream.min(new SpeedComparator())
                .each(vehicleField,new Debug("#### slowest vehicle"));

        Stream slowDriversStream = slowVehiclesStream.project(driverField)
                .each(driverField,new Debug("#### slowest driver"));

        vehiclesStream.max(new SpeedComparator())
                .each(vehicleField,new Debug("#### fastest vehicle"))
                .project(driverField)
                .each(driverField,new Debug("#### fastest driver"));

        vehiclesStream.minBy(Vehicle.FIELD_NAME, new EfficiencyComparator())
                .each(vehicleField, new Debug("#### least efficient vehicle"));

        vehiclesStream.maxBy(Vehicle.FIELD_NAME, new EfficiencyComparator())
                .each(vehicleField, new Debug("#### most efficient vehicle"));

        return topology.build();

    }

    public static void main(String[] args) {
        StormTopology topology = buildVehiclesTopology();
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.setNumWorkers(3);
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mydemo",conf,topology);
//        StormSubmitter.submitTopologyWithProgressBar("vehicles-topology", conf, topology);
    }

    static class Driver implements Serializable {
        static final String FIELD_NAME = "driver";
        final String name;
        final int id;

        Driver(String name, int id) {
            this.name = name;
            this.id = id;
        }

        @Override
        public String toString() {
            return "Driver{" +
                    "name='" + name + '\'' +
                    ", id=" + id +
                    '}';
        }
    }

    static class Vehicle implements Serializable {
        static final String FIELD_NAME = "vehicle";
        final String name;
        final int maxSpeed;
        final double efficiency;

        public Vehicle(String name, int maxSpeed, double efficiency) {
            this.name = name;
            this.maxSpeed = maxSpeed;
            this.efficiency = efficiency;
        }

        @Override
        public String toString() {
            return "Vehicle{" +
                    "name='" + name + '\'' +
                    ", maxSpeed=" + maxSpeed +
                    ", efficiency=" + efficiency +
                    '}';
        }

        public static List<Object>[] generateVehicles(int count) {
            List<Object>[] vehicles = new List[count];
            for (int i = 0; i < count; i++) {
                int id = i - 1;
                vehicles[i] =
                        (new Values(
                                new Vehicle("Vehicle-" + id, ThreadLocalRandom.current().nextInt(0, 100), ThreadLocalRandom.current().nextDouble(1, 5)),
                                new Driver("Driver-" + id, id)
                        ));
            }
            return vehicles;
        }
    }

}
