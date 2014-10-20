package com.moz.qless;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class ClientCreation {
  public static Client create(final JedisPool jedisPool) {
    final Jedis jedis = jedisPool.getResource();
    try {
      jedis.flushDB();
    } finally {
      jedisPool.returnResource(jedis);
    }

    return new Client(jedisPool);
  }
}
