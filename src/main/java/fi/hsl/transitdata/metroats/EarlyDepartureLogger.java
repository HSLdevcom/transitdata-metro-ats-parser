package fi.hsl.transitdata.metroats;

import fi.hsl.transitdata.metroats.models.MetroEstimate;
import fi.hsl.transitdata.metroats.models.MetroStopEstimate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Helper class for logging cases where estimated departure time is earlier than scheduled
 */
public class EarlyDepartureLogger {
    private Logger logger = LoggerFactory.getLogger(EarlyDepartureLogger.class);

    private final Duration interval;

    private long startTime = System.nanoTime();
    private Set<String> tripsWithEarlyDeparture = new HashSet<>();

    public EarlyDepartureLogger(Duration interval) {
        this.interval = interval;
    }

    public void checkEarlyDeparture(MetroEstimate metroEstimate) {
        if (Duration.ofNanos(System.nanoTime() - startTime).compareTo(interval) >= 0) {
            logger.info("{} trips within last {} minutes where estimated departure was before scheduled departure", tripsWithEarlyDeparture.size(), interval.toMinutes());

            startTime = System.nanoTime();
            tripsWithEarlyDeparture.clear();
        }

        MetroStopEstimate first = metroEstimate.routeRows.get(0);
        if (first != null) {
            final Optional<ZonedDateTime> scheduled = MetroUtils.parseMetroAtsDatetime(first.departureTimePlanned);
            final Optional<ZonedDateTime> estimated = MetroUtils.parseMetroAtsDatetime(first.departureTimeForecast);

            if (scheduled.isPresent() && estimated.isPresent() && scheduled.get().isBefore(estimated.get())) {
                tripsWithEarlyDeparture.add(metroEstimate.routeName+"-"+metroEstimate.beginTime+"-"+metroEstimate.trainType);
            }
        }
    }
}
