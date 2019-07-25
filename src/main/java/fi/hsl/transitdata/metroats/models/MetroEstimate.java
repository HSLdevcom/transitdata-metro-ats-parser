package fi.hsl.transitdata.metroats.models;

import fi.hsl.transitdata.metroats.MetroUtils;

import java.util.List;
import java.util.Optional;

public class MetroEstimate {
    public String routeName;
    public MetroTrainType trainType;
    public MetroProgress journeySectionprogress;
    public String beginTime;
    public String endTime;
    public List<MetroStopEstimate> routeRows;

    public void setBeginTime(String beginTime) {
        Optional<String> maybeBeginTime = MetroUtils.toUtcDatetime(beginTime);
        this.beginTime = maybeBeginTime.orElse(null);
    }

    public void setEndTime(String endTime) {
        Optional<String> maybeEndTime = MetroUtils.toUtcDatetime(endTime);
        this.endTime = maybeEndTime.orElse(null);
    }
}
