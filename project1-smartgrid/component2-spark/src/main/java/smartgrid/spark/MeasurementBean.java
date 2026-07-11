package smartgrid.spark;

import java.io.Serializable;

/**
 * Bean tipado de una medición, usado para tipar el stream con Encoders.bean
 * y poder aplicar mapGroupsWithState (Query 1 de EnergyAnalytics).
 */
public class MeasurementBean implements Serializable {

    private String nodeId;
    private String nodeType;
    private String districtId;
    private double value;
    private long timestamp;

    public MeasurementBean() {
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeType() {
        return nodeType;
    }

    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }

    public String getDistrictId() {
        return districtId;
    }

    public void setDistrictId(String districtId) {
        this.districtId = districtId;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
