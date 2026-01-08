package fi.hsl.transitdata.metroats;

import static org.junit.Assert.*;

import fi.hsl.common.files.FileUtils;
import fi.hsl.transitdata.metroats.models.MetroEstimate;
import fi.hsl.transitdata.metroats.models.MetroStopEstimate;
import java.io.InputStream;
import java.net.URL;
import java.util.Optional;
import org.junit.Test;

public class MetroEstimatesFactoryTest {

    @Test
    public void testDateTimeConversion() throws Exception {
        InputStream stream = getClass().getResourceAsStream("/metro.json");
        String json = FileUtils.readFileFromStreamOrThrow(stream);
        Optional<MetroEstimate> maybeMetroEstimate = MetroEstimatesFactory.parsePayload(json.getBytes());
        assertTrue(maybeMetroEstimate.isPresent());
        MetroEstimate metroEstimate = maybeMetroEstimate.get();
        assertEquals("2019-07-09T05:06:30.404Z", metroEstimate.beginTime);
        assertEquals("2019-07-09T05:47:10.404Z", metroEstimate.endTime);
        assertTrue(metroEstimate.routeRows.size() > 0);
        MetroStopEstimate metroStopEstimate = metroEstimate.routeRows.get(0);
        assertEquals(25866852, metroStopEstimate.routerowId);
        assertEquals("2019-07-09T05:06:05.404Z", metroStopEstimate.arrivalTimePlanned);
        assertEquals("2019-07-09T05:06:13.941Z", metroStopEstimate.arrivalTimeForecast);
        assertEquals("2019-07-09T05:00:44.470Z", metroStopEstimate.arrivalTimeMeasured);
        assertEquals("2019-07-09T05:06:30.404Z", metroStopEstimate.departureTimePlanned);
        assertEquals("2019-07-09T05:06:13.941Z", metroStopEstimate.departureTimeForecast);
        assertEquals("2019-07-09T05:06:32.578Z", metroStopEstimate.departureTimeMeasured);
    }

    @Test
    public void testDropPayloadWithNoMeasuredDepartureTimeForFirstStation() throws Exception {
        // The API surprisingly uses "null" instead of null in JSON.
        String json = """
                {
                  "routeName": "M1",
                  "beginTime": "2023-01-01T12:01:02.345Z",
                  "routeRows": [
                    {
                      "station": "KIV",
                      "departureTimeMeasured": "null"
                    }
                  ]
                }""";
        Optional<MetroEstimate> result = MetroEstimatesFactory.parsePayload(json.getBytes());
        assertFalse("Should filter out messages without a measured departure time for first station",
                result.isPresent());
    }

    @Test
    public void testKeepPayloadWithMeasuredDepartureTimeForFirstStation() throws Exception {
        String json = """
                {
                  "routeName": "M1",
                  "beginTime": "2023-01-01T12:01:02.345Z",
                  "routeRows": [
                    {
                      "station": "KIV",
                      "departureTimeMeasured": "2023-01-01T12:01:05.678Z"
                    }
                  ]
                }""";
        Optional<MetroEstimate> result = MetroEstimatesFactory.parsePayload(json.getBytes());
        assertTrue("Should accept messages with a measured departure time for first station", result.isPresent());
    }

    @Test
    public void testKeepPayloadThatCancelsJourneyEvenWithNoMeasuredDepartureTimeForFirstStation() throws Exception {
        // The API surprisingly uses "null" instead of null in JSON.
        String json = """
                {
                  "routeName": "M1",
                  "beginTime": "2023-01-01T12:01:02.345Z",
                  "journeySectionprogress": "CANCELLED",
                  "routeRows": [
                    {
                      "station": "KIV",
                      "departureTimeMeasured": "null"
                    }
                  ]
                }""";
        Optional<MetroEstimate> result = MetroEstimatesFactory.parsePayload(json.getBytes());
        assertTrue(
                "Should accept messages that cancel the whole vehicle journey even if they do not have a measured departure time for first station",
                result.isPresent());
    }
}
