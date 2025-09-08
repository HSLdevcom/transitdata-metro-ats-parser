package fi.hsl.transitdata.metroats;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.pulsar.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static fi.hsl.transitdata.metroats.Checks.checkEither;
import static fi.hsl.transitdata.metroats.RedisClusterProperties.redisClusterProperties;
import static redis.clients.jedis.Protocol.DEFAULT_DATABASE;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Starting Metro-ats Parser");
        Config config = ConfigParser.createConfig();
        try (PulsarApplication app = PulsarApplication.newInstance(config)) {

            PulsarApplicationContext context = app.getContext();
            boolean addedTripsEnabled = config.getBoolean("application.addedTripsEnabled");
            var jedisExecutor = createJedisExecutor(context);

            MetroEstimatesFactory metroEstimatesFactory = new MetroEstimatesFactory(context, addedTripsEnabled, jedisExecutor);
            MessageHandler router = new MessageHandler(context, metroEstimatesFactory);

            log.info("Start handling the messages");
            app.launchWithHandler(router);
        } catch (Exception e) {
            log.error("Exception at main", e);
        }
    }

    private static JedisExecutor createJedisExecutor(PulsarApplicationContext context) {
        final var config = context.getConfig();
        final var redisEnabled = config.getBoolean("redis.enabled");
        final var redisClusterEnabled = config.getBoolean("redisCluster.enabled");
        checkEither(redisEnabled, redisClusterEnabled,
                "Exactly one of 'redis.enabled' or 'redisCluster.enabled' must be true");

        if (redisEnabled) {
            final var jedis = context.getJedis();
            return new JedisExecutor() {
                @Override
                public <T> T execute(Function<Jedis, T> action) {
                    synchronized (jedis) {
                        return action.apply(jedis);
                    }
                }
            };
        } else {
            final var properties = redisClusterProperties(config);
            final var pool = createJedisSentinelPool(properties);
            final var jedisExecutor = new JedisExecutor() {
                @Override
                public <T> T execute(Function<Jedis, T> action) {
                    try (final var jedis = pool.getResource()) {
                        return action.apply(jedis);
                    }
                }
            };

            if (properties.healthCheck) {
                context.getHealthServer()
                        .addCheck(redisCustomHealthCheck(jedisExecutor));
            }

            return jedisExecutor;
        }
    }

    private static JedisSentinelPool createJedisSentinelPool(RedisClusterProperties properties) {
        return new JedisSentinelPool(
                properties.masterName,
                properties.sentinels,
                properties.jedisPoolConfig(),
                (int) properties.connectionTimeout.toMillis(),
                (int) properties.socketTimeout.toMillis(),
                null,
                DEFAULT_DATABASE
        );
    }

    private static BooleanSupplier redisCustomHealthCheck(JedisExecutor jedisExecutor) {
        return () -> jedisExecutor.execute(jedis -> {
            try {
                final var maybePong = jedis.ping();
                if (maybePong.equals("PONG")) {
                    return true;
                } else {
                    log.error("jedis.ping() returned: {}", maybePong);
                }
            } catch (Exception e) {
                log.error("Exception in custom health check for redis connection", e);
            }

            return false;
        });
    }
}