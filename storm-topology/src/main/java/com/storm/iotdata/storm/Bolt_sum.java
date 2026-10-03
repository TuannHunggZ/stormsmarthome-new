package com.storm.iotdata.storm;

import com.storm.iotdata.functions.DB_store;
import com.storm.iotdata.models.HouseData;
import com.storm.iotdata.models.HouseProp;
import com.storm.iotdata.models.PlugData;
import com.storm.iotdata.models.RedisAnomalyPublisher;
import com.storm.iotdata.models.StormConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

/**
 * Storm bolt that aggregates completed plug slices into house slices.
 * Household aggregation, monitoring and notification publishing are excluded.
 * House outliers are tracked with HouseProp and published through Redis.
 */
public class Bolt_sum extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_sum.class);

    private final Integer windowSizeMinutes;
    private final Map<String, Map<Integer, Map<String, PlugData>>> plugDataBySlice;
    private final Map<String, HouseProp> housePropByHouse;
    private final int anomalyThresholdPercent;
    private final boolean houseCheckMax;
    private final boolean houseCheckAvg;
    private final boolean houseCheckMin;
    private transient RedisAnomalyPublisher redisPublisher;
    private transient OutputCollector collector;

    /**
     * Creates a house aggregation bolt for one configured window.
     *
     * @param windowSizeMinutes Window size handled by this bolt, in minutes.
     */
    public Bolt_sum(int windowSizeMinutes) {
        this.windowSizeMinutes = windowSizeMinutes;
        this.plugDataBySlice = new HashMap<String, Map<Integer, Map<String, PlugData>>>();
        this.housePropByHouse = new HashMap<String, HouseProp>();
        this.anomalyThresholdPercent = StormConfig.getAnomalyThresholdPercent();
        this.houseCheckMax = StormConfig.isHouseCheckMax();
        this.houseCheckAvg = StormConfig.isHouseCheckAvg();
        this.houseCheckMin = StormConfig.isHouseCheckMin();
    }

    /**
     * Initializes the collector and Redis publisher.
     *
     * @param stormConf Storm configuration map.
     * @param context Topology context.
     * @param collector Storm output collector.
     */
    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.redisPublisher = new RedisAnomalyPublisher();
        this.redisPublisher.initialize();
        LOGGER.info("Bolt_sum initialized for window {}m", windowSizeMinutes);
    }

    /**
     * Accepts PlugData from Bolt_average and flushes house aggregates on punctuation.
     *
     * @param tuple Incoming Storm tuple.
     */
    @Override
    public void execute(Tuple tuple) {
        try {
            if (getPunctuationStreamId().equals(tuple.getSourceStreamId())) {
                processPunctuation(tuple);
                collector.ack(tuple);
            } else if ("data".equals(tuple.getSourceStreamId())
                && PlugData.class.getSimpleName().equals(tuple.getValueByField("type"))) {
                processPlugData(tuple);
                collector.ack(tuple);
            } else {
                LOGGER.warn("Received tuple from unsupported stream {}", tuple.getSourceStreamId());
                collector.fail(tuple);
            }
        } catch (Exception exception) {
            LOGGER.error("Failed to process tuple from stream {}", tuple.getSourceStreamId(), exception);
            collector.fail(tuple);
        }
    }

    /**
     * Declares house aggregate and punctuation output streams.
     *
     * @param declarer Storm declarer.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("data", new Fields("type", "data"));
        declarer.declareStream(getPunctuationStreamId(), new Fields("windowSize", "triggerTimestampMillis"));
    }

    /**
     * Closes Redis and clears local aggregation state.
     */
    @Override
    public void cleanup() {
        LOGGER.info("Cleaning up Bolt_sum for window {}m", windowSizeMinutes);
        if (redisPublisher != null) {
            redisPublisher.close();
        }
        plugDataBySlice.clear();
        housePropByHouse.clear();
    }

    private void processPlugData(Tuple tuple) {
        PlugData plugData = (PlugData) tuple.getValueByField("data");
        Map<Integer, Map<String, PlugData>> houseDataBySlice = plugDataBySlice.getOrDefault(
            plugData.getSliceId(),
            new HashMap<Integer, Map<String, PlugData>>()
        );
        Map<String, PlugData> plugDataByHouse = houseDataBySlice.getOrDefault(
            plugData.getHouseId(),
            new HashMap<String, PlugData>()
        );

        plugDataByHouse.put(String.valueOf(plugData.getPlugUniqueId()), plugData);
        houseDataBySlice.put(plugData.getHouseId(), plugDataByHouse);
        plugDataBySlice.put(plugData.getSliceId(), houseDataBySlice);
    }

    private void processPunctuation(Tuple tuple) {
        int punctuationWindowSize = tuple.getIntegerByField("windowSize");
        long triggerTimestampMillis = tuple.getLongByField("triggerTimestampMillis");

        if (punctuationWindowSize != windowSizeMinutes) {
            LOGGER.debug("Ignoring punctuation for window {}m in bolt configured for {}m", punctuationWindowSize, windowSizeMinutes);
            return;
        }

        Stack<HouseData> houseDataToSave = new Stack<HouseData>();
        for (Map<Integer, Map<String, PlugData>> houseDataBySlice : plugDataBySlice.values()) {
            for (Map.Entry<Integer, Map<String, PlugData>> houseEntry : houseDataBySlice.entrySet()) {
                HouseData houseData = createHouseData(houseEntry.getKey(), houseEntry.getValue());
                collector.emit("data", tuple, new Values(HouseData.class.getSimpleName(), houseData));
                houseDataToSave.push(houseData);
            }
        }

        if (DB_store.pushHouseData(houseDataToSave, new File("./tmp/houseData2db-" + windowSizeMinutes + ".lck"))) {
            for (HouseData houseData : houseDataToSave) {
                updateHouseAnomaly(houseData, triggerTimestampMillis);
                houseData.save();
            }
            plugDataBySlice.clear();
        }

        collector.emit(getPunctuationStreamId(), tuple, new Values(windowSizeMinutes, triggerTimestampMillis));
        LOGGER.info("Processed punctuation for window {}m with {} house records", windowSizeMinutes, houseDataToSave.size());
    }

    private HouseData createHouseData(int houseId, Map<String, PlugData> plugDataByHouse) {
        PlugData firstPlugData = plugDataByHouse.values().iterator().next();
        double houseValue = 0.0d;
        for (PlugData plugData : plugDataByHouse.values()) {
            houseValue += plugData.getAvg();
        }
        return new HouseData(
            houseId,
            firstPlugData.getYear(),
            firstPlugData.getMonth(),
            firstPlugData.getDay(),
            firstPlugData.getSliceIndex(),
            firstPlugData.getSliceGap(),
            houseValue
        );
    }

    private void updateHouseAnomaly(HouseData houseData, long triggerTimestampMillis) {
        double currentAverage = houseData.getAvg();
        if (currentAverage == 0.0d) {
            return;
        }

        String houseKey = houseData.getHouseUniqueId() + "-" + windowSizeMinutes;
        HouseProp houseProp = housePropByHouse.getOrDefault(
            houseKey,
            new HouseProp(houseData.getHouseId(), windowSizeMinutes)
        );
        if (houseProp.getCount() > 0.0d) {
            checkHouseAnomaly(houseData, currentAverage, houseProp, triggerTimestampMillis);
        }
        houseProp.addValue(currentAverage);
        housePropByHouse.put(houseKey, houseProp);
    }

    private void checkHouseAnomaly(HouseData houseData, double currentAverage, HouseProp houseProp, long triggerTimestampMillis) {
        double threshold = anomalyThresholdPercent / 100.0d;
        if (houseCheckMax && (currentAverage - houseProp.getMax()) >= houseProp.getMax() * threshold) {
            publishHouseAnomaly("MAX", houseData, currentAverage, houseProp, triggerTimestampMillis);
        }
        if (houseCheckAvg && (currentAverage - houseProp.getAvg()) >= houseProp.getAvg() * threshold) {
            publishHouseAnomaly("AVG", houseData, currentAverage, houseProp, triggerTimestampMillis);
        }
        if (houseCheckMin && (houseProp.getMin() - currentAverage) >= houseProp.getMin() * threshold) {
            publishHouseAnomaly("MIN", houseData, currentAverage, houseProp, triggerTimestampMillis);
        }
    }

    private void publishHouseAnomaly(String anomalyType, HouseData houseData, double currentAverage, HouseProp houseProp, long triggerTimestampMillis) {
        Map<String, Object> event = new LinkedHashMap<String, Object>();
        event.put("type", "HOUSE_ANOMALY");
        event.put("anomalyType", anomalyType);
        event.put("windowSize", windowSizeMinutes);
        event.put("timestamp", triggerTimestampMillis);
        event.put("houseId", houseData.getHouseId());
        event.put("householdId", null);
        event.put("plugId", null);
        event.put("value", currentAverage);
        event.put("avg", houseProp.getAvg());
        event.put("min", houseProp.getMin());
        event.put("max", houseProp.getMax());
        event.put("anomalyThresholdPercent", anomalyThresholdPercent);

        redisPublisher.publish(StormConfig.getHouseAnomalyChannel(), event);
        LOGGER.warn("House anomaly detected: type={} windowSize={} houseId={} value={} avg={} min={} max={} triggerTimestampMillis={}",
            anomalyType, windowSizeMinutes, houseData.getHouseId(), currentAverage,
            houseProp.getAvg(), houseProp.getMin(), houseProp.getMax(), triggerTimestampMillis);
    }

    private String getPunctuationStreamId() {
        return "punctuation-" + windowSizeMinutes + "m";
    }
}
