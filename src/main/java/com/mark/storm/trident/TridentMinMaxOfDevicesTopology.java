package com.mark.storm.trident;

import com.mark.storm.spout.RandomNumberGeneratorSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;


/**
 * Created by lulei on 2018/2/28.
 */
public class TridentMinMaxOfDevicesTopology {

    public static StormTopology buildDevicesTopology(){
        String deviceID = "device-id";
        String count = "count";
        Fields allFields = new Fields(deviceID, count);

        RandomNumberGeneratorSpout spout = new RandomNumberGeneratorSpout(allFields,10,1000);

        TridentTopology topology =new TridentTopology();
        Stream devicesStream = topology.newStream("spout",spout)
                .each(allFields,new Debug("### devices"));

        devicesStream.minBy(deviceID)
                .each(allFields,new Debug("#### device with min id"));

        devicesStream.maxBy(count)
                .each(allFields,new Debug("#### device with max count"));

        return topology.build();
    }

    public static StormTopology buildVehiclesTopology(){
        Fields driverField = new Fields(TridentMinMaxOfVehiclesTopology.Driver.FIELD_NAME);
        Fields vehicleField = new Fields(TridentMinMaxOfVehiclesTopology.Vehicle.FIELD_NAME);
        Fields allFields = new Fields(TridentMinMaxOfVehiclesTopology.Vehicle.FIELD_NAME, TridentMinMaxOfVehiclesTopology.Driver.FIELD_NAME);

        FixedBatchSpout spout = new FixedBatchSpout(allFields,10, TridentMinMaxOfVehiclesTopology.Vehicle.generateVehicles(20));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream vehiclesStream = topology.newStream("spout1",spout)
                .each(allFields,new Debug("#### vehicles"));

        Stream slowVehiclesStream = vehiclesStream.min(new TridentMinMaxOfVehiclesTopology.SpeedComparator())
                .each(vehicleField,new Debug("#### slowest vehicle"));

        Stream slowDriversStream = slowVehiclesStream.project(driverField)
                .each(driverField,new Debug("#### slowest driver"));

        vehiclesStream.max(new TridentMinMaxOfVehiclesTopology.SpeedComparator())
                .each(vehicleField,new Debug("#### fastest vehicle"))
                .project(driverField)
                .each(driverField,new Debug("####  fastest driver"));



        return topology.build();
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        StormTopology topology = buildDevicesTopology();
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar("devices-topology", conf, topology);
    }


}
