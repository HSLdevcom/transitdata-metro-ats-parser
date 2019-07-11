package fi.hsl.transitdata.metroats.models;

import java.util.List;

public class MetroEstimate {
    public int schemaVersion;
    public String routeName;
    public MetroTrainType trainType;
    public MetroProgress journeySectionprogress;
    public String beginTime;
    public String endTime;
    public List<MetroStopEstimate> routeRows;
}
