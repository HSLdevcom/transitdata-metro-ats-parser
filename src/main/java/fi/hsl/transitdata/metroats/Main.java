package fi.hsl.transitdata.metroats;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Starting Metro-ats Parser");
        Config config = ConfigParser.createConfig();
        try (PulsarApplication app = PulsarApplication.newInstance(config)) {

            PulsarApplicationContext context = app.getContext();
            boolean addedTripsEnabled = config.getBoolean("application.addedTripsEnabled");;

            MetroEstimatesFactory metroEstimatesFactory = new MetroEstimatesFactory(context, addedTripsEnabled);
            MessageHandler router = new MessageHandler(context, metroEstimatesFactory);

            log.info("Start handling the messages");
            app.launchWithHandler(router);
        } catch (Exception e) {
            log.error("Exception at main", e);
        }
    }
}