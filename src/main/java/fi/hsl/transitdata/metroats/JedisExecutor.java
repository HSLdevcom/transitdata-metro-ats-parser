package fi.hsl.transitdata.metroats;

import redis.clients.jedis.Jedis;

import java.util.function.Function;

@FunctionalInterface
public interface JedisExecutor {

    <T> T execute(Function<Jedis, T> action);
}
