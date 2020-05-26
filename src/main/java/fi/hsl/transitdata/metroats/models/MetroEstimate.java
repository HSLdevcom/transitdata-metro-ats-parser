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
    public String originalJourneyStartTime;
    public List<MetroStopEstimate> routeRows;

    public void setBeginTime(String beginTime) {
        Optional<String> maybeBeginTime = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(beginTime);
        this.beginTime = maybeBeginTime.orElse(null);
    }

    public void setEndTime(String endTime) {
        Optional<String> maybeEndTime = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(endTime);
        this.endTime = maybeEndTime.orElse(null);
    }

    public void setOriginalJourneyStartTime(String originalJourneyStartTime) {
        Optional<String> maybeOriginalJourneyStartTime = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(originalJourneyStartTime);
        this.originalJourneyStartTime = maybeOriginalJourneyStartTime.orElse(null);
    }
}
