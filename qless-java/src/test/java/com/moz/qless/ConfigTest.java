package com.moz.qless;

import java.io.IOException;
import java.util.Map;

import com.moz.qless.client.ClientHelper;
import com.moz.qless.lua.LuaConfigParameter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class ConfigTest {
  private final JedisPool jedisPool = new JedisPool(ClientHelper.DEFAULT_HOSTNAME);
  private Client client;
  private final String testKey = "foo";
  private final String testKeyValue = "1";

  @Before
  public void before() throws IOException {
      final Jedis jedis = this.jedisPool.getResource();
      try {
          jedis.flushDB();
      } finally {
          this.jedisPool.returnResource(jedis);
      }

      this.client = new Client(this.jedisPool);
  }

  @Test
  public void getAll() throws IOException {
    final Map<String, Object> config = this.client.getConfig().getMap();

    Assert.assertEquals(ClientHelper.DEFAULT_HEARTBEAT,
        config.get(LuaConfigParameter.HEARTBEAT.toString()));
    Assert.assertEquals(ClientHelper.DEFAULT_APPLICATION,
        config.get(LuaConfigParameter.APPLICATION.toString()));
    Assert.assertEquals(ClientHelper.DEFAULT_GRACE_PERIOD,
        config.get(LuaConfigParameter.GRACE_PERIOD.toString()));
    Assert.assertEquals(ClientHelper.DEFAULT_JOBS_HISTORY,
        config.get(LuaConfigParameter.JOBS_HISTORY.toString()));
    Assert.assertEquals(ClientHelper.DEFAULT_STATS_HISTORY,
        config.get(LuaConfigParameter.STATS_HISTORY.toString()));
    Assert.assertEquals(ClientHelper.DEFAULT_HISTOGRAM_HISTORY,
        config.get(LuaConfigParameter.HISTOGRAM_HISTORY.toString()));
    Assert.assertEquals(ClientHelper.DEFAULT_JOBS_HISTORY_COUNT,
        config.get(LuaConfigParameter.JOBS_HISTORY_COUNT.toString()));
  }

  @Test
  public void setGetUnset() throws IOException {
    final String itemName = "testing";
    this.client.getConfig().put(itemName, this.testKey);
    Assert.assertEquals(this.testKey, this.client.getConfig().get(itemName));
    Assert.assertEquals(this.testKey, this.client.getConfig().getMap().get(itemName));

    this.client.getConfig().remove(itemName);
    Assert.assertEquals(null, this.client.getConfig().get(itemName));
  }

  @Test
  public void clear() throws IOException {
    final Map<String, Object> originalConfig = this.client.getConfig().getMap();
    for (final String key : originalConfig.keySet()) {
      this.client.getConfig().put(key, this.testKeyValue);
    }

    this.client.getConfig().clear();
    Assert.assertEquals(originalConfig, this.client.getConfig().getMap());
  }

  @Test
  public void len() throws IOException {
    Assert.assertEquals(7, this.client.getConfig().getMap().size());
  }

  @Test
  public void contains() throws IOException {
    Assert.assertFalse(this.client.getConfig().getMap().containsKey(this.testKey));
    this.client.getConfig().put(this.testKey, this.testKeyValue);
    Assert.assertTrue(this.client.getConfig().getMap().containsKey(this.testKey));
  }

  @Test
  public void get() throws IOException {
    this.client.getConfig().put(this.testKey, this.testKeyValue);
    Assert.assertEquals(this.testKeyValue, this.client.getConfig().get(this.testKey));
  }

  @Test
  public void pop() throws IOException {
    this.client.getConfig().put(this.testKey, this.testKeyValue);
    Assert.assertEquals(this.testKeyValue, this.client.getConfig().pop(this.testKey));
    Assert.assertFalse(this.client.getConfig().getMap().containsKey(this.testKey));
  }

  @Test
  public void update() throws IOException {
    this.client.getConfig().put(this.testKey, this.testKeyValue);
    Assert.assertEquals(this.testKeyValue, this.client.getConfig().pop(this.testKey));
    this.client.getConfig().put(this.testKey, "2");
    Assert.assertEquals("2", this.client.getConfig().pop(this.testKey));
  }
}
