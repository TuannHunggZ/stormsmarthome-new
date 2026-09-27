package com.storm.iotdata.models;

/**
 * House aggregate for one date and time slice.
 */
public class HouseData extends TimeSlice {

    private static final long serialVersionUID = 1L;

    private final Integer houseId;
    private Double value;
    public Double predictionLatencyMillis = 0.0d;
    private Boolean saved;

    public HouseData(Integer houseId, String year, String month, String day, Integer sliceIndex, Integer sliceGap, Double value) {
        super(year, month, day, sliceIndex, sliceGap);
        this.houseId = houseId;
        this.value = value;
        this.saved = false;
    }

    public Integer getHouseId() {
        return houseId;
    }

    public Double getValue() {
        return value;
    }

    public Double getAvg() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
        this.saved = false;
    }

    public boolean isSaved() {
        return saved;
    }

    public HouseData save() {
        this.saved = true;
        return this;
    }

    public String getUniqueId() {
        return String.format("%d-%s-%s-%s-%d-%d", houseId, year, month, day, sliceGap, sliceIndex);
    }

    public String getHouseUniqueId() {
        return String.valueOf(houseId);
    }
}
