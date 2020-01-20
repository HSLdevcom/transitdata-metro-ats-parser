package fi.hsl.transitdata.metroats;

import fi.hsl.common.files.FileUtils;
import fi.hsl.transitdata.metroats.models.MetroEstimate;
import fi.hsl.transitdata.metroats.models.MetroStopEstimate;
import org.junit.Test;

import java.io.InputStream;
import java.net.URL;
import java.util.Optional;

import static org.junit.Assert.*;

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
}
