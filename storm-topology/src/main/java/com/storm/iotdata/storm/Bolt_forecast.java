package com.storm.iotdata.storm;

import com.storm.iotdata.functions.DB_store;
import com.storm.iotdata.models.HouseData;
import com.storm.iotdata.models.PlugData;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * Storm sink that creates house and plug forecasts from completed aggregates.
 * Household data, monitoring and notification publishing are intentionally excluded.
 * Forecast records are persisted with prediction latency in milliseconds.
 */
public class Bolt_forecast extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_forecast.class);

    private final Integer windowSizeMinutes;
    private final Map<String, PlugData> plugDataList;
    private final Map<String, HouseData> houseDataList;
    private transient OutputCollector collector;

    /**
     * Creates a forecast bolt for one configured window.
     *
     * @param windowSizeMinutes Window size handled by this bolt, in minutes.
     */
    public Bolt_forecast(Integer windowSizeMinutes) {
        this.windowSizeMinutes = windowSizeMinutes;
        this.plugDataList = new HashMap<String, PlugData>();
        this.houseDataList = new HashMap<String, HouseData>();
    }

    /**
     * Initializes the forecast bolt.
     *
     * @param stormConf Storm configuration map.
     * @param context Topology context.
     * @param collector Storm output collector.
     */
    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        LOGGER.info("Bolt_forecast initialized for window {}m", windowSizeMinutes);
    }

    /**
     * Caches aggregate data and forecasts it when punctuation closes the window.
     *
     * @param tuple Incoming aggregate or punctuation tuple.
     */
    @Override
    public void execute(Tuple tuple) {
        try {
            if ("data".equals(tuple.getSourceStreamId())) {
                processData(tuple);
                collector.ack(tuple);
            } else if (getPunctuationStreamId().equals(tuple.getSourceStreamId())) {
                processPunctuation(tuple);
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
     * This bolt persists forecasts and does not emit downstream streams.
     *
     * @param declarer Storm declarer.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Forecast records are persisted by DB_store.
    }

    /**
     * Clears pending forecast state during shutdown.
     */
    @Override
    public void cleanup() {
        LOGGER.info("Cleaning up Bolt_forecast for window {}m", windowSizeMinutes);
        plugDataList.clear();
        houseDataList.clear();
    }

    private void processData(Tuple tuple) {
        Object data = tuple.getValueByField("data");
        if (data instanceof PlugData) {
            PlugData plugData = (PlugData) data;
            plugDataList.put(plugData.getUniqueId(), plugData);
        } else if (data instanceof HouseData) {
            HouseData houseData = (HouseData) data;
            houseDataList.put(houseData.getUniqueId(), houseData);
        } else {
            LOGGER.debug("Ignoring unsupported forecast data type {}", data.getClass().getName());
        }
    }

    private void processPunctuation(Tuple tuple) {
        int punctuationWindowSize = tuple.getIntegerByField("windowSize");
        long triggerTimestampMillis = tuple.getLongByField("triggerTimestampMillis");
        if (punctuationWindowSize != windowSizeMinutes) {
            LOGGER.debug("Ignoring punctuation for window {}m in bolt configured for {}m", punctuationWindowSize, windowSizeMinutes);
            return;
        }

        Stack<PlugData> plugForecasts = new Stack<PlugData>();
        for (PlugData plugData : plugDataList.values()) {
            plugForecasts.push(createPlugForecast(plugData, triggerTimestampMillis));
        }
        Stack<HouseData> houseForecasts = new Stack<HouseData>();
        for (HouseData houseData : houseDataList.values()) {
            houseForecasts.push(createHouseForecast(houseData, triggerTimestampMillis));
        }

        boolean plugSaved = DB_store.pushPlugDataForecast(
            plugForecasts,
            new File("./tmp/plugForecast2db-" + windowSizeMinutes + ".lck")
        );
        boolean houseSaved = DB_store.pushHouseDataForecast(
            houseForecasts,
            new File("./tmp/houseForecast2db-" + windowSizeMinutes + ".lck")
        );
        if (plugSaved) {
            plugDataList.clear();
        }
        if (houseSaved) {
            houseDataList.clear();
        }
        LOGGER.info(
            "Forecasted {} plug records and {} house records for window {}m",
            plugForecasts.size(),
            houseForecasts.size(),
            windowSizeMinutes
        );
    }

    private PlugData createPlugForecast(PlugData data, long triggerTimestampMillis) {
        double forecastValue = data.getAvg();
        double median = getPlugMedian(DB_store.queryBefore(data));
        if (median > 0.0d) {
            forecastValue = (forecastValue + median) / 2.0d;
        }
        PlugData forecast = new PlugData(
            data.getHouseId(),
            data.getHouseholdId(),
            data.getPlugId(),
            data.getYear(),
            data.getMonth(),
            data.getDay(),
            data.getSliceIndex() + 2,
            data.getSliceGap()
        );
        forecast.value = forecastValue;
        forecast.count = 1.0d;
        forecast.predictionLatencyMillis = (double) (System.currentTimeMillis() - triggerTimestampMillis);
        return forecast;
    }

    private HouseData createHouseForecast(HouseData data, long triggerTimestampMillis) {
        double forecastValue = data.getAvg();
        double median = getHouseMedian(DB_store.queryBefore(data));
        if (median > 0.0d) {
            forecastValue = (forecastValue + median) / 2.0d;
        }
        HouseData forecast = new HouseData(
            data.getHouseId(),
            data.getYear(),
            data.getMonth(),
            data.getDay(),
            data.getSliceIndex() + 2,
            data.getSliceGap(),
            forecastValue
        );
        forecast.predictionLatencyMillis = (double) (System.currentTimeMillis() - triggerTimestampMillis);
        return forecast;
    }

    private static double getPlugMedian(HashMap<String, PlugData> data) {
        ArrayList<PlugData> values = new ArrayList<PlugData>(data.values());
        values.sort(new Comparator<PlugData>() {
            @Override
            public int compare(PlugData first, PlugData second) {
                return Double.compare(first.getAvg(), second.getAvg());
            }
        });
        return getMiddleValue(values);
    }

    private static double getHouseMedian(HashMap<String, HouseData> data) {
        ArrayList<HouseData> values = new ArrayList<HouseData>(data.values());
        values.sort(new Comparator<HouseData>() {
            @Override
            public int compare(HouseData first, HouseData second) {
                return Double.compare(first.getAvg(), second.getAvg());
            }
        });
        return getMiddleValue(values);
    }

    private static double getMiddleValue(ArrayList<? extends Object> values) {
        if (values.isEmpty()) {
            return 0.0d;
        }
        int middle = values.size() / 2;
        double upper = getAverageValue(values.get(middle));
        if (values.size() % 2 != 0) {
            return upper;
        }
        return (upper + getAverageValue(values.get(middle - 1))) / 2.0d;
    }

    private static double getAverageValue(Object value) {
        if (value instanceof PlugData) {
            return ((PlugData) value).getAvg();
        }
        return ((HouseData) value).getAvg();
    }

    private String getPunctuationStreamId() {
        return "punctuation-" + windowSizeMinutes + "m";
    }
}
