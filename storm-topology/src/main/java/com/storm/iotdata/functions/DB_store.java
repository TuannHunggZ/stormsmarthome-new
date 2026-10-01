package com.storm.iotdata.functions;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storm.iotdata.models.PlugData;
import com.storm.iotdata.models.HouseData;
import com.storm.iotdata.models.StormConfig;

/**
 * PostgreSQL persistence helper that follows the asynchronous locker pattern
 * used by the legacy topology.
 */
public class DB_store {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DB_store.class);

	/**
     * Opens a PostgreSQL connection using the topology configuration.
     *
     * @return A connection with auto-commit disabled.
	 */
    public static Connection initConnection() throws SQLException {
		try {
            String jdbcUrl = StormConfig.getJdbcUrl();
            Connection connection = DriverManager.getConnection(
                jdbcUrl,
                StormConfig.getJdbcUser(),
                StormConfig.getJdbcPassword()
            );
            connection.setAutoCommit(false);
            LOGGER.info("Connected to PostgreSQL at {}", jdbcUrl);
            return connection;
		} catch (SQLException exception) {
			throw new IllegalStateException("Unable to initialize PostgreSQL connection", exception);
		}
	}

    /**
     * Starts asynchronous persistence unless another worker owns the locker.
     *
     * @param dataList Completed plug aggregates.
     * @param locker Per-window persistence lock file.
     * @return true when a persistence worker was started.
     */
	public static boolean pushPlugData(Stack<PlugData> dataList, File locker) {
		try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            }
            new PlugData2DB(dataList, locker).start();
            return true;
        } catch (Exception exception) {
            LOGGER.error("Failed to start plug data persistence worker", exception);
            return false;
        }
	}

    /**
     * Starts asynchronous persistence for completed house aggregates.
     *
     * @param dataList Completed house aggregates.
     * @param locker Per-window persistence lock file.
     * @return true when a persistence worker was started.
     */
    public static boolean pushHouseData(Stack<HouseData> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseData2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception exception) {
            LOGGER.error("Failed to start house data persistence worker", exception);
            return false;
        }
	}

	public static HashMap<String, PlugData> queryBefore(PlugData data) {
            HashMap<String, PlugData> result = new HashMap<String, PlugData>();
            String sql = "SELECT * "
                + "FROM plug_data WHERE house_id=? AND household_id=? AND plug_id=? "
                + "AND slice_gap=? AND slice_index=?";
            try (Connection connection = initConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, data.getHouseId());
                statement.setInt(2, data.getHouseholdId());
                statement.setInt(3, data.getPlugId());
                statement.setInt(4, data.getSliceGap());
                statement.setInt(5, data.getSliceIndex() + 2);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        PlugData previous = new PlugData(
                            resultSet.getInt("house_id"), resultSet.getInt("household_id"), resultSet.getInt("plug_id"),
                            resultSet.getString("year"), resultSet.getString("month"), resultSet.getString("day"),
                            resultSet.getInt("slice_index"), resultSet.getInt("slice_gap")
                        );
                        previous.value = resultSet.getDouble("avg");
                        previous.count = 1.0d;
                        result.put(previous.getUniqueId(), previous);
                    }
                }
            } catch (SQLException exception) {
                LOGGER.error("Failed to query plug history before slice {}", data.getSliceId(), exception);
            }
            return result;
        }

    	public static HashMap<String, HouseData> queryBefore(HouseData data) {
            HashMap<String, HouseData> result = new HashMap<String, HouseData>();
            String sql = "SELECT * FROM house_data "
                + "WHERE house_id=? AND slice_gap=? AND slice_index=?";
            try (Connection connection = initConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, data.getHouseId());
                statement.setInt(2, data.getSliceGap());
                statement.setInt(3, data.getSliceIndex() + 2);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        HouseData previous = new HouseData(
                            resultSet.getInt("house_id"), resultSet.getString("year"), resultSet.getString("month"),
                            resultSet.getString("day"), resultSet.getInt("slice_index"), resultSet.getInt("slice_gap"),
                            resultSet.getDouble("avg")
                        );
                        result.put(previous.getUniqueId(), previous);
                    }
                }
            } catch (SQLException exception) {
                LOGGER.error("Failed to query house history before slice {}", data.getSliceId(), exception);
            }
            return result;
        }

    	public static boolean pushPlugDataForecast(Stack<PlugData> dataList, File locker) {
            try{
                if (locker.exists() || dataList.isEmpty()) {
                    return false;
                } else {
                    new PlugForecast2DB(dataList, locker).start();
                    return true;
                }
            } catch (Exception exception) {
                LOGGER.error("Failed to start plug forecast persistence worker", exception);
                return false;
            }
        }

    	public static boolean pushHouseDataForecast(Stack<HouseData> dataList, File locker) {
            try {
                if (locker.exists() || dataList.isEmpty()) {
                    return false;
                }
            } catch (Exception exception) {
                LOGGER.error("Failed to start house forecast persistence worker", exception);
                return false;
            }
            new HouseForecast2DB(dataList, locker).start();
            return true;
        }
}

/**
 * Writes one completed plug-data batch and retries after a temporary failure.
 */
class PlugData2DB extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlugData2DB.class);

    private Stack<PlugData> dataList;
    private File locker;

    public PlugData2DB(Stack<PlugData> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();

            Connection conn = DB_store.initConnection();
            try (PreparedStatement tempSql = conn.prepareStatement(
                "INSERT INTO plug_data " +
                "(house_id, household_id, plug_id, year, month, day, slice_gap, slice_index, value, count, avg) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (house_id, household_id, plug_id, year, month, day, slice_gap, slice_index) " +
                "DO UPDATE SET " +
                "value = EXCLUDED.value, " +
                "count = EXCLUDED.count, " +
                "avg = EXCLUDED.avg"
            )) {
                for (PlugData data : dataList) {
                    tempSql.setInt(1, data.getHouseId());
                    tempSql.setInt(2, data.getHouseholdId());
                    tempSql.setInt(3, data.getPlugId());
                    tempSql.setString(4, data.getYear());
                    tempSql.setString(5, data.getMonth());
                    tempSql.setString(6, data.getDay());
                    tempSql.setInt(7, data.getSliceGap());
                    tempSql.setInt(8, data.getSliceIndex());
                    tempSql.setDouble(9, data.getValue());
                    tempSql.setDouble(10, data.getCount());
                    tempSql.setDouble(11, data.getAvg());
                    tempSql.addBatch();
                }
                tempSql.executeBatch();
            }
            conn.commit();
            conn.close();
            locker.delete();
            dataList = null;
        } catch (Exception exception) {
            LOGGER.error("Failed to persist plug data; retrying", exception);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Plug data persistence retry was interrupted", interruptedException);
            }

            locker.delete();
            new PlugData2DB(dataList, locker).start();
        }
    }
}

/**
 * Writes completed house aggregates using the same retry pattern as plug data.
 */
class HouseData2DB extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(HouseData2DB.class);
    private final Stack<HouseData> dataList;
    private final File locker;

    HouseData2DB(Stack<HouseData> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            try (Connection connection = DB_store.initConnection();
                 PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO house_data "
                         + "(house_id, year, month, day, slice_gap, slice_index, avg) "
                         + "VALUES (?, ?, ?, ?, ?, ?, ?) "
                         + "ON CONFLICT (house_id, year, month, day, slice_gap, slice_index) "
                         + "DO UPDATE SET avg = EXCLUDED.avg")) {
                for (HouseData data : dataList) {
                    statement.setInt(1, data.getHouseId());
                    statement.setString(2, data.getYear());
                    statement.setString(3, data.getMonth());
                    statement.setString(4, data.getDay());
                    statement.setInt(5, data.getSliceGap());
                    statement.setInt(6, data.getSliceIndex());
                    statement.setDouble(7, data.getAvg());
                    statement.addBatch();
                }
                statement.executeBatch();
                connection.commit();

            }
            locker.delete();
        } catch (Exception exception) {
            LOGGER.error("Failed to persist house data; retrying", exception);
            retry();
        }
    }

    private void retry() {
        locker.delete();
        new HouseData2DB(dataList, locker).start();
    }
}

class PlugForecast2DB extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlugForecast2DB.class);
    private final Stack<PlugData> dataList;
    private final File locker;

    PlugForecast2DB(Stack<PlugData> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            try (Connection connection = DB_store.initConnection();
                 PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO plug_data_forecast "
                         + "(house_id, household_id, plug_id, year, month, day, slice_gap, slice_index, avg, prediction_latency) "
                         + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                         + "ON CONFLICT (house_id, household_id, plug_id, year, month, day, slice_gap, slice_index) "
                         + "DO UPDATE SET avg = EXCLUDED.avg, prediction_latency = EXCLUDED.prediction_latency")) {
                for (PlugData data : dataList) {
                    statement.setInt(1, data.getHouseId());
                    statement.setInt(2, data.getHouseholdId());
                    statement.setInt(3, data.getPlugId());
                    statement.setString(4, data.getYear());
                    statement.setString(5, data.getMonth());
                    statement.setString(6, data.getDay());
                    statement.setInt(7, data.getSliceGap());
                    statement.setInt(8, data.getSliceIndex());
                    statement.setDouble(9, data.getAvg());
                    statement.setDouble(10, data.predictionLatencyMillis);
                    statement.addBatch();
                }
                statement.executeBatch();
                connection.commit();
            }
            locker.delete();
        } catch (Exception exception) {
            LOGGER.error("Failed to persist plug forecast; retrying", exception);
            locker.delete();
            new PlugForecast2DB(dataList, locker).start();
        }
    }
}

class HouseForecast2DB extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(HouseForecast2DB.class);
    private final Stack<HouseData> dataList;
    private final File locker;

    HouseForecast2DB(Stack<HouseData> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            try (Connection connection = DB_store.initConnection();
                 PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO house_data_forecast "
                         + "(house_id, year, month, day, slice_gap, slice_index, avg, prediction_latency) "
                         + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                         + "ON CONFLICT (house_id, year, month, day, slice_gap, slice_index) "
                         + "DO UPDATE SET avg = EXCLUDED.avg, prediction_latency = EXCLUDED.prediction_latency")) {
                for (HouseData data : dataList) {
                    statement.setInt(1, data.getHouseId());
                    statement.setString(2, data.getYear());
                    statement.setString(3, data.getMonth());
                    statement.setString(4, data.getDay());
                    statement.setInt(5, data.getSliceGap());
                    statement.setInt(6, data.getSliceIndex());
                    statement.setDouble(7, data.getAvg());
                    statement.setDouble(8, data.predictionLatencyMillis);
                    statement.addBatch();
                }
                statement.executeBatch();
                connection.commit();
            }
            locker.delete();
        } catch (Exception exception) {
            LOGGER.error("Failed to persist house forecast; retrying", exception);
            locker.delete();
            new HouseForecast2DB(dataList, locker).start();
        }
    }
}
