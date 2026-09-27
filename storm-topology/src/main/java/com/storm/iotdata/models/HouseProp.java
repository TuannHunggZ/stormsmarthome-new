package com.storm.iotdata.models;

import java.io.Serializable;

/**
 * Rolling house statistics used for anomaly detection.
 */
public class HouseProp implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int houseId;
    private final int sliceGap;
    private double min;
    private double avg;
    private double max;
    private double count;

    public HouseProp(int houseId, int sliceGap) {
        this.houseId = houseId;
        this.sliceGap = sliceGap;
    }

    public int getHouseId() {
        return houseId;
    }

    public int getSliceGap() {
        return sliceGap;
    }

    public double getMin() {
        return min;
    }

    public double getAvg() {
        return avg;
    }

    public double getMax() {
        return max;
    }

    public double getCount() {
        return count;
    }

    public HouseProp addValue(double value) {
        if (value == 0.0d) {
            return this;
        }
        if (count == 0.0d) {
            min = value;
            avg = value;
            max = value;
            count = 1.0d;
        } else {
            avg = (avg * count + value) / (++count);
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        return this;
    }

    public HouseProp save() {
        return this;
    }

    public String getUniqueId() {
        return String.format("%d-%d", houseId, sliceGap);
    }

    public String getHouseUniqueId() {
        return String.valueOf(houseId);
    }
}
