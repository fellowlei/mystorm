package com.mark.storm;

import com.mark.storm.bolt.PrinterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.concurrent.TimeUnit;

/**
 * Created by lulei on 2018/2/27.
 */
public class JoinBoltExample {
    public static void main(String[] args) {
        FeederSpout genderSpout = new FeederSpout(new Fields("id", "gender"));
        FeederSpout ageSpout = new FeederSpout(new Fields("id", "age"));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("genderSpout", genderSpout);
        builder.setSpout("ageSpout", ageSpout);


        // inner join of 'age' and 'gender' records on 'id' field
        JoinBolt joiner = new JoinBolt("genderSpout", "id")
                .join("ageSpout",    "id", "genderSpout")
                .select ("genderSpout:id,ageSpout:id,gender,age")
                .withTumblingWindow( new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS) );

        builder.setBolt("joiner", joiner)
                .fieldsGrouping("genderSpout", new Fields("id"))
                .fieldsGrouping("ageSpout", new Fields("id"))         ;

        builder.setBolt("printer", new PrinterBolt() ).shuffleGrouping("joiner");

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setNumWorkers(2);

        LocalCluster cluster =new LocalCluster();
        cluster.submitTopology("mydemo",conf,builder.createTopology());
    }

    private static void generateAgeData(FeederSpout ageSpout) {
        for (int i = 9; i >= 0; i--) {
            ageSpout.feed(new Values(i, i + 20));
        }
    }

    private static void generateGenderData(FeederSpout genderSpout) {
        for (int i = 0; i < 10; i++) {
            String gender;
            if (i % 2 == 0) {
                gender = "male";
            }
            else {
                gender = "female";
            }
            genderSpout.feed(new Values(i, gender));
        }
    }
}
