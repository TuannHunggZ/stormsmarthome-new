package com.storm.iotdata;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import com.storm.iotdata.models.HouseData;
import com.storm.iotdata.models.PlugData;
import com.storm.iotdata.models.StormConfig;
import com.storm.iotdata.storm.Bolt_average;
import com.storm.iotdata.storm.Bolt_forecast;
import com.storm.iotdata.storm.Bolt_split;
import com.storm.iotdata.storm.Bolt_sum;
import com.storm.iotdata.storm.Spout_data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds and submits the MQTT to PostgreSQL Storm topology.
 */
public class MainTopo {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainTopo.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout-data", new Spout_data(), 1);

        BoltDeclarer splitBolt = builder.setBolt("bolt-split", new Bolt_split(), 1);
        splitBolt.shuffleGrouping("spout-data", "data");

        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            splitBolt.shuffleGrouping("spout-data", "punctuation-" + windowSize + "m");

            String averageBoltId = "bolt-average-" + windowSize + "m";
            BoltDeclarer boltDeclarer = builder.setBolt(averageBoltId, new Bolt_average(windowSize), 1);
            boltDeclarer.shuffleGrouping("bolt-split", "window-" + windowSize + "m");
            boltDeclarer.shuffleGrouping("bolt-split", "punctuation-" + windowSize + "m");

            String sumBoltId = "bolt-sum-" + windowSize + "m";
            BoltDeclarer sumBoltDeclarer = builder.setBolt(sumBoltId, new Bolt_sum(windowSize), 1);
            sumBoltDeclarer.shuffleGrouping(averageBoltId, "data");
            sumBoltDeclarer.shuffleGrouping(averageBoltId, "punctuation-" + windowSize + "m");

            String forecastBoltId = "bolt-forecast-" + windowSize + "m";
            BoltDeclarer forecastBoltDeclarer = builder.setBolt(
                forecastBoltId,
                new Bolt_forecast(windowSize),
                1
            );
            forecastBoltDeclarer.shuffleGrouping(averageBoltId, "data");
            forecastBoltDeclarer.shuffleGrouping(sumBoltId, "data");
            forecastBoltDeclarer.shuffleGrouping(sumBoltId, "punctuation-" + windowSize + "m");
        }

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(4);
        config.registerSerialization(PlugData.class);
        config.registerSerialization(HouseData.class);

        LOGGER.info("Submitting iot-smarthome topology with windows {}", StormConfig.getTimeSliceMinutes());
        StormSubmitter.submitTopology("iot-smarthome", config, builder.createTopology());
    }
}
