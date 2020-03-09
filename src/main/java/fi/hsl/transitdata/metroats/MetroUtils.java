package fi.hsl.transitdata.metroats;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.transitdata.PubtransFactory;
import org.apache.pulsar.shade.com.google.common.collect.ArrayListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class MetroUtils {
    private static final Logger log = LoggerFactory.getLogger(MetroUtils.class);

    private static final HashMap<String, String> shortNameByStopNumber = new HashMap<>();
    private static final ArrayListMultimap<String, String> stopNumbersByShortName = ArrayListMultimap.create();
    private static final List<String> shortNames = new ArrayList<>();
    private static final DateTimeFormatter dateTimeFormatter;
    private static final DateTimeFormatter metroAtsDateTimeFormatter;
    private static final DateTimeFormatter pubtransDateTimeFormatter;
    private static final DateTimeFormatter utcDateTimeFormatter;
    private static final ZoneId metroAtsZoneId;
    private static final ZoneId pubtransZoneId;
    private static final ZoneId utcZoneId;

    static {
        final Config stopsConfig = ConfigParser.createConfig("metro_stops.conf");
        stopsConfig.getObjectList("metroStops")
                .forEach(stopConfigObject -> {
                    final Config stopConfig = stopConfigObject.toConfig();
                    final String shortName = stopConfig.getString("shortName");
                    final List<String> stopNumbers = stopConfig.getStringList("stopNumbers");
                    shortNames.add(shortName);
                    stopNumbers.forEach(stopNumber -> {
                        shortNameByStopNumber.put(stopNumber, shortName);
                        stopNumbersByShortName.put(shortName, stopNumber);
                    });
                });
        final Config config = ConfigParser.createConfig();
        final String metroAtsTimeZone = config.getString("application.metroAtstimezone");
        final String pubtransTimeZone = config.getString("application.pubtransTimezone");
        metroAtsZoneId = ZoneId.of(metroAtsTimeZone);
        pubtransZoneId = ZoneId.of(pubtransTimeZone);
        utcZoneId = ZoneId.of("UTC");
        dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        metroAtsDateTimeFormatter = dateTimeFormatter.withZone(metroAtsZoneId);
        pubtransDateTimeFormatter = dateTimeFormatter.withZone(pubtransZoneId);
        utcDateTimeFormatter = dateTimeFormatter.withZone(utcZoneId);
    }

    private MetroUtils() {}

    public static Optional<String> getShortName(final String stopNumber) {
        return Optional.ofNullable(shortNameByStopNumber.get(stopNumber));
    }

    public static List<String> getStopNumbers(final String shortName) {
        return stopNumbersByShortName.get(shortName);
    }

    public static Optional<Integer> getJoreDirection(final String startStop, final String endStop) {
        final int startStopIndex = shortNames.indexOf(startStop);
        final int endStopIndex = shortNames.indexOf(endStop);
        if (startStopIndex == -1 || endStopIndex == -1 || startStopIndex == endStopIndex) {
            return Optional.empty();
        }
        return Optional.of(startStopIndex < endStopIndex ? PubtransFactory.JORE_DIRECTION_ID_OUTBOUND : PubtransFactory.JORE_DIRECTION_ID_INBOUND);
    }

    public static Optional<String> getStopNumber(final String shortName, final int joreDirection) {
        List<String> stopNumbers = getStopNumbers(shortName);
        if (joreDirection < 1 || joreDirection > 2 || stopNumbers.isEmpty()) {
            return Optional.empty();
        }
        // The first stop number corresponds Jore direction 1 and the second stop number corresponds Jore direction 2
        String stopNumber;
        try {
            stopNumber = stopNumbers.get(joreDirection - 1);
        } catch (Exception e) {
            return Optional.empty();
        }
        return Optional.ofNullable(stopNumber);
    }

    public static Optional<String> getStopNumber(final String shortName, final String startStopShortName, final String endStopShortName) {
        final Optional<Integer> joreDirection = getJoreDirection(startStopShortName, endStopShortName);
        return joreDirection.isPresent() ? getStopNumber(shortName, joreDirection.get()) : Optional.empty();
    }

    public static Optional<String> convertDatetime(final String datetime, final DateTimeFormatter formatter, final ZoneId toZoneId) {
        if (datetime == null || datetime.isEmpty() || datetime.equals("null")) {
            return Optional.empty();
        }

        try {
            final ZonedDateTime zonedDateTime = ZonedDateTime.parse(datetime, formatter).withZoneSameInstant(toZoneId);
            return Optional.of(zonedDateTime.format(dateTimeFormatter));
        }
        catch (Exception e) {
            log.error(String.format("Failed to parse datetime from %s", datetime), e);
            return Optional.empty();
        }
    }

    public static Optional<String> convertMetroAtsDatetimeToUtcDatetime(final String metroAtsDatetime) {
        return convertDatetime(metroAtsDatetime, metroAtsDateTimeFormatter, utcZoneId);
    }

    public static Optional<String> convertUtcDatetimeToPubtransDatetime(final String utcDatetime) {
        return convertDatetime(utcDatetime, utcDateTimeFormatter, pubtransZoneId);
    }

    public static Optional<String> getRouteName(String startStopName, String endStopName) {
        Optional<String> routeName = getRouteNameFromStops(startStopName, endStopName);
        if (routeName.isPresent()) {
            return routeName;
        } else {
            return getRouteNameFromStops(endStopName, startStopName);
        }
    }

    private static Optional<String> getRouteNameFromStops(String stop1, String stop2) {
        if ("MM".equals(stop1)) {
            if ("TAP".equals(stop2)) {
                return Optional.of("31M2");
            } else if ("MAK".equals(stop2)) {
                return Optional.of("31M2M");
            } else if ("IK".equals(stop2)) {
                return Optional.of("31M2B");
            } else {
                return Optional.empty();
            }
        } else if ("VS".equals(stop1)) {
            if ("MAK".equals(stop2)) {
                return Optional.of("31M1");
            } else if ("IK".equals(stop2)) {
                return Optional.of("31M1B");
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }
}
