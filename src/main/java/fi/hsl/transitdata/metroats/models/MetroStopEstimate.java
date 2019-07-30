package fi.hsl.transitdata.metroats.models;

import fi.hsl.transitdata.metroats.MetroUtils;

import java.util.Optional;

public class MetroStopEstimate {
    public long routerowId;
    public String station;
    public String platform;
    public String arrivalTimePlanned;
    public String arrivalTimeForecast;
    public String arrivalTimeMeasured;
    public String departureTimePlanned;
    public String departureTimeForecast;
    public String departureTimeMeasured;
    public String source;
    public MetroProgress rowProgress;

    public void setArrivalTimePlanned(String arrivalTimePlanned) {
        Optional<String> maybeArrivalTimePlanned = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(arrivalTimePlanned);
        this.arrivalTimePlanned = maybeArrivalTimePlanned.orElse(null);
    }

    public void setArrivalTimeForecast(String arrivalTimeForecast) {
        Optional<String> maybeArrivalTimeForecast = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(arrivalTimeForecast);
        this.arrivalTimeForecast = maybeArrivalTimeForecast.orElse(null);
    }

    public void setArrivalTimeMeasured(String arrivalTimeMeasured) {
        Optional<String> maybeArrivalTimeMeasured = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(arrivalTimeMeasured);
        this.arrivalTimeMeasured = maybeArrivalTimeMeasured.orElse(null);
    }

    public void setDepartureTimePlanned(String departureTimePlanned) {
        Optional<String> maybeDepartureTimePlanned = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(departureTimePlanned);
        this.departureTimePlanned = maybeDepartureTimePlanned.orElse(null);
    }

    public void setDepartureTimeForecast(String departureTimeForecast) {
        Optional<String> maybeDepartureTimeForecast = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(departureTimeForecast);
        this.departureTimeForecast = maybeDepartureTimeForecast.orElse(null);
    }

    public void setDepartureTimeMeasured(String departureTimeMeasured) {
        Optional<String> maybeDepartureTimeMeasured = MetroUtils.convertMetroAtsDatetimeToUtcDatetime(departureTimeMeasured);
        this.departureTimeMeasured = maybeDepartureTimeMeasured.orElse(null);
    }
}
