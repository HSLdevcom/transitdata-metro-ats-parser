package fi.hsl.transitdata.metroats;

import fi.hsl.common.transitdata.proto.InternalMessages;
import fi.hsl.common.transitdata.proto.MetroAtsProtos;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MetroEstimateStopEstimatesFactory{
    private static final Logger log = LoggerFactory.getLogger(MetroEstimateStopEstimatesFactory.class);

    public Optional<List<InternalMessages.StopEstimate>> toStopEstimates(final Message message) {
        try {
            final long timestamp = message.getEventTime();
            return Optional.of(toStopEstimates(message.getData(), timestamp));

        } catch (Exception e) {
            log.warn("Failed to produce metro estimate stop estimates.", e);
        }
        return Optional.empty();
    }

    private List<InternalMessages.StopEstimate> toStopEstimates(byte[] data, final long timestamp) throws Exception {
        MetroAtsProtos.MetroEstimate metroEstimate = MetroAtsProtos.MetroEstimate.parseFrom(data);
        return toStopEstimates(metroEstimate, timestamp);
    }
    public List<InternalMessages.StopEstimate> toStopEstimates(MetroAtsProtos.MetroEstimate metroEstimate, final long timestamp) throws Exception {

        //T type trains are not passenger trains -> do not produce estimates
        if (metroEstimate.hasTrainType() && metroEstimate.getTrainType() == MetroAtsProtos.MetroTrainType.T) {
            return Collections.emptyList();
        }

        List<InternalMessages.StopEstimate> metroStopEstimates = metroEstimate.getMetroRowsList().stream()
                .flatMap(metroStopEstimate -> {
                    final int stopSequence = metroEstimate.getMetroRowsList().indexOf(metroStopEstimate) + 1;
                    return toStopEstimates(metroEstimate, metroStopEstimate, stopSequence, timestamp).stream();
                })
                .collect(Collectors.toList());

        //If more than one stop has been cancelled for a single metro, assume that cancellations are valid
        if (metroStopEstimates.stream().filter(metroStopEstimate -> metroStopEstimate.getStatus() == InternalMessages.StopEstimate.Status.SKIPPED).count() > 2) {
            return metroStopEstimates;
        } else {
            return metroStopEstimates.stream().map(metroStopEstimate -> {
                //Change invalid skipped status to scheduled
                if (metroStopEstimate.getStatus() == InternalMessages.StopEstimate.Status.SKIPPED) {
                    return metroStopEstimate.toBuilder().setStatus(InternalMessages.StopEstimate.Status.SCHEDULED).build();
                } else {
                    return metroStopEstimate;
                }
            }).collect(Collectors.toList());
        }
    }

    private List<InternalMessages.StopEstimate> toStopEstimates(final MetroAtsProtos.MetroEstimate metroEstimate, final MetroAtsProtos.MetroStopEstimate metroStopEstimate, final int stopSequence, final long timestamp) {
        final Optional<InternalMessages.StopEstimate> maybeArrivalStopEstimate = toStopEstimate(metroEstimate, metroStopEstimate, stopSequence, timestamp, InternalMessages.StopEstimate.Type.ARRIVAL);
        final Optional<InternalMessages.StopEstimate> maybeDepartureStopEstimate = toStopEstimate(metroEstimate, metroStopEstimate, stopSequence, timestamp, InternalMessages.StopEstimate.Type.DEPARTURE);
        List<InternalMessages.StopEstimate> stopEstimates = new ArrayList<>();
        maybeArrivalStopEstimate.ifPresent(stopEstimates::add);
        maybeDepartureStopEstimate.ifPresent(stopEstimates::add);
        return stopEstimates;
    }

    private Optional<InternalMessages.StopEstimate> toStopEstimate(final MetroAtsProtos.MetroEstimate metroEstimate, final MetroAtsProtos.MetroStopEstimate metroStopEstimate, final int stopSequence, final long timestamp, final InternalMessages.StopEstimate.Type type) {
        InternalMessages.StopEstimate.Builder stopEstimateBuilder = InternalMessages.StopEstimate.newBuilder();
        InternalMessages.TripInfo.Builder tripBuilder = InternalMessages.TripInfo.newBuilder();

        // TripInfo
        tripBuilder.setTripId(metroEstimate.getDvjId());
        tripBuilder.setOperatingDay(metroEstimate.getOperatingDay());
        tripBuilder.setRouteId(metroEstimate.getRouteName());
        tripBuilder.setDirectionId(Integer.parseInt(metroEstimate.getDirection()));
        tripBuilder.setStartTime(metroEstimate.getStartTime());
        tripBuilder.setScheduleType(metroEstimate.hasScheduled() && !metroEstimate.getScheduled() ? // If metro trip is not scheduled, assume that it is added to the schedule
                InternalMessages.TripInfo.ScheduleType.ADDED :
                InternalMessages.TripInfo.ScheduleType.SCHEDULED);

        // StopEstimate
        if (metroEstimate.getJourneySectionprogress().equals(MetroAtsProtos.MetroProgress.CANCELLED)) {
            return Optional.empty();
        }
        stopEstimateBuilder.setSchemaVersion(stopEstimateBuilder.getSchemaVersion());
        stopEstimateBuilder.setTripInfo(tripBuilder.build());
        stopEstimateBuilder.setStopId(metroStopEstimate.getStopNumber());
        stopEstimateBuilder.setStopSequence(stopSequence);
        // Status
        Optional<InternalMessages.StopEstimate.Status> maybeStopEstimateStatus = getStopEstimateStatus(metroStopEstimate.getRowProgress());
        if (maybeStopEstimateStatus.isPresent()) {
            stopEstimateBuilder.setStatus(maybeStopEstimateStatus.get());
        } else {
            log.warn("Stop estimate had no rowProgress, stop number: {}, route name: {}, operating day: {}, start time: {}, direction: {}", metroStopEstimate.getStopNumber(), metroEstimate.getRouteName(), metroEstimate.getOperatingDay(), metroEstimate.getStartTime(), metroEstimate.getDirection());
            return Optional.empty();
        }
        stopEstimateBuilder.setType(type);


        // EstimatedTimeUtcMs & ScheduledTimeUtcMs
        if (metroStopEstimate.getArrivalTimePlanned().isEmpty() || metroStopEstimate.getDepartureTimePlanned().isEmpty()) {
            log.warn("Stop estimate had no planned arrival or departure time (stop number: {}, route name: {}, operating day: {}, start time: {}, direction: {})", metroStopEstimate.getStopNumber(), metroEstimate.getRouteName(), metroEstimate.getOperatingDay(), metroEstimate.getStartTime(), metroEstimate.getDirection());
            return Optional.empty();
        }

        boolean isForecastMissing = false;
        switch (type) {
            case ARRIVAL:
                stopEstimateBuilder.setScheduledTimeUtcMs(ZonedDateTime.parse(metroStopEstimate.getArrivalTimePlanned()).toInstant().toEpochMilli());

                String arrivalTime = !metroStopEstimate.getArrivalTimeMeasured().isEmpty()
                    ? metroStopEstimate.getArrivalTimeMeasured()
                    : !metroStopEstimate.getArrivalTimeForecast().isEmpty()
                        ? metroStopEstimate.getArrivalTimeForecast()
                        : null;
                if (arrivalTime != null) {
                    stopEstimateBuilder.setEstimatedTimeUtcMs(ZonedDateTime.parse(arrivalTime).toInstant().toEpochMilli());
                } else {
                    isForecastMissing = true;
                }
                break;
            case DEPARTURE:
                stopEstimateBuilder.setScheduledTimeUtcMs(ZonedDateTime.parse(metroStopEstimate.getDepartureTimePlanned()).toInstant().toEpochMilli());
                String departureTime = !metroStopEstimate.getDepartureTimeMeasured().isEmpty()
                    ? metroStopEstimate.getDepartureTimeMeasured()
                    : !metroStopEstimate.getDepartureTimeForecast().isEmpty()
                        ? metroStopEstimate.getDepartureTimeForecast()
                        : null;
                if (departureTime != null) {
                    stopEstimateBuilder.setEstimatedTimeUtcMs(ZonedDateTime.parse(departureTime).toInstant().toEpochMilli());
                } else {
                    isForecastMissing = true;
                }
                break;
            default:
                log.warn("Unrecognized type {}.", type);
                break;
        }
        if (isForecastMissing && stopEstimateBuilder.getStatus() != InternalMessages.StopEstimate.Status.SKIPPED) {
            stopEstimateBuilder.setStatus(InternalMessages.StopEstimate.Status.NO_DATA);
        }

        if (shouldIgnoreKoivusaari(metroStopEstimate)) {
            log.debug("Ignoring metro stop estimate from Koivusaari");
            return Optional.empty();
        }

        // LastModifiedUtcMs
        stopEstimateBuilder.setLastModifiedUtcMs(timestamp);

        return Optional.of(stopEstimateBuilder.build());
    }

    //"Hack" for ignoring data from Koivusaari station when it is closed
    private static boolean shouldIgnoreKoivusaari(MetroAtsProtos.MetroStopEstimate metroStopEstimate) {
        final LocalDate date = ZonedDateTime.parse(metroStopEstimate.getArrivalTimePlanned()).withZoneSameInstant(ZoneId.of("Europe/Helsinki")).toLocalDate();

        return "KOS".equals(metroStopEstimate.getStation()) && //station = Koivusaari
                date.isBefore(LocalDate.of(2020, 8, 5).plusDays(1)) && //Before 5.8.2020
                date.isAfter(LocalDate.of(2020, 6, 1).minusDays(1)); //After 1.6.2020
    }

    private Optional<InternalMessages.StopEstimate.Status> getStopEstimateStatus(MetroAtsProtos.MetroProgress metroProgress) {
        switch (metroProgress) {
            case SCHEDULED:
            case INPROGRESS:
            case COMPLETED:
                return Optional.of(InternalMessages.StopEstimate.Status.SCHEDULED);
            case CANCELLED:
                return Optional.of(InternalMessages.StopEstimate.Status.SKIPPED);
            default:
                log.warn("Unrecognized MetroProgress {}.", metroProgress);
                return Optional.empty();
        }
    }
}
