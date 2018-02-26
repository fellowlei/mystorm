package com.mark.storm;

import com.mark.storm.bolt.SingleJoinBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Created by lulei on 2018/2/26.
 */
public class SingleJoinExample {
    public static void main(String[] args) {
        FeederSpout genderSpout = new FeederSpout(new Fields("id","gender"));
        FeederSpout ageSpout = new FeederSpout(new Fields("id","age"));

        TopologyBuilder builder =new TopologyBuilder();
        builder.setSpout("gender",genderSpout);
        builder.setSpout("age",ageSpout);
        builder.setBolt("first",new SingleJoinBolt()).fieldsGrouping("gender",new Fields("id")).fieldsGrouping("age",new Fields("id"));

        Config config =new Config();
        config.setDebug(true);
        config.setNumWorkers(3);

        LocalCluster cluster =new LocalCluster();
        cluster.submitTopology("mywork",config,builder.createTopology());

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

        for (int i = 9; i >= 0; i--) {
            ageSpout.feed(new Values(i, i + 20));
        }

    }
}
