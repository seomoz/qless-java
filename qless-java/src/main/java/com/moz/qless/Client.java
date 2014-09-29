package com.moz.qless;

import com.moz.qless.core.LuaScript;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPool;

/**
 * Basic qless client object.
 *
 */
public class Client {
  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);
  protected JedisPool jedisPool;
  protected LuaScript luaScript;
  protected Config config;
  protected Events events;
  protected Jobs jobs;
  protected Queues queues;

  public Client(final JedisPool jedisPool) {
  }

  public Object call(final String command, final String... args) {
    return null;
  }
}
