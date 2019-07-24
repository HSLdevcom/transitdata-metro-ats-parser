package fi.hsl.transitdata.metroats;

import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

public class MetroUtilsTest {

    @Test
    public void getShortName() {
        assertEquals("MAK", MetroUtils.getShortName("2314601").get());
        assertEquals("MAK", MetroUtils.getShortName("2314602").get());
        assertFalse(MetroUtils.getShortName("1234567").isPresent());
    }

    @Test
    public void getStopNumbers() {
        final List<String> stopNumbers = MetroUtils.getStopNumbers("MAK");
        assertEquals(2, stopNumbers.size());
        assertEquals("2314601", stopNumbers.get(0));
        assertEquals("2314602", stopNumbers.get(1));
        assertTrue(MetroUtils.getStopNumbers("FOO").isEmpty());
    }

    @Test
    public void getJoreDirection() {
        assertEquals(1, MetroUtils.getJoreDirection("MAK", "VS").get().intValue());
        assertEquals(1, MetroUtils.getJoreDirection("TAP", "MM").get().intValue());
        assertEquals(1, MetroUtils.getJoreDirection("IK", "VS").get().intValue());
        assertEquals(1, MetroUtils.getJoreDirection("IK", "MM").get().intValue());
        assertEquals(2, MetroUtils.getJoreDirection("VS", "MAK").get().intValue());
        assertEquals(2, MetroUtils.getJoreDirection("MM", "TAP").get().intValue());
        assertEquals(2, MetroUtils.getJoreDirection("VS", "IK").get().intValue());
        assertEquals(2, MetroUtils.getJoreDirection("MM", "IK").get().intValue());
        assertFalse(MetroUtils.getJoreDirection("MAK", "MAK").isPresent());
        assertFalse(MetroUtils.getJoreDirection("MAK", "FOO").isPresent());
        assertFalse(MetroUtils.getJoreDirection("FOO", "MAK").isPresent());
    }

    @Test
    public void getStopNumber() {
        assertEquals("2211601", MetroUtils.getStopNumber("TAP", "MAK", "VS").get());
        assertEquals("1020604", MetroUtils.getStopNumber("HY", "MM", "TAP").get());
        assertFalse(MetroUtils.getStopNumber("TAP", "MAK", "MAK").isPresent());
        assertFalse(MetroUtils.getStopNumber("TAP", "MAK", "FOO").isPresent());
        assertFalse(MetroUtils.getStopNumber("FOO", "MAK", "VS").isPresent());
    }

    @Test
    public void testLocalDateTimeToUtcDatetime() {
        final String localDateTime = "2019-07-18T15:38:30.632Z";
        final Optional<String> maybeUtcDateTime = MetroUtils.toUtcDatetime(localDateTime);
        assertTrue(maybeUtcDateTime.isPresent());
        assertEquals("2019-07-18T12:38:30.632Z", maybeUtcDateTime.get());
    }

    @Test
    public void testInvalidDateTimeToUtcDatetime() {
        final String localDateTime = "2019-07-18T15:38:30Z";
        final Optional<String> maybeUtcDateTime = MetroUtils.toUtcDatetime(localDateTime);
        assertFalse(maybeUtcDateTime.isPresent());
    }

    @Test
    public void testUtcDatetimeToLocalDateTime() {
        final String utcDateTime = "2019-07-18T12:38:30.632Z";
        final Optional<String> maybeLocalDateTime = MetroUtils.toLocalDatetime(utcDateTime);
        assertTrue(maybeLocalDateTime.isPresent());
        assertEquals("2019-07-18T15:38:30.632Z", maybeLocalDateTime.get());
    }
}
