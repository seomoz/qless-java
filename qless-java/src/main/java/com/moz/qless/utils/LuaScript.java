package com.moz.qless.utils;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.SafeEncoder;

import com.google.common.io.Resources;

/**
 * A single unified core script to interact with qless-core.
 *
 */
public class LuaScript {
  private final Logger LOGGER = LoggerFactory.getLogger(LuaScript.class);
  public static final String SCRIPT = "qless.lua";
  private final JedisPool jedisPool;
  protected byte[] scriptContents;
  protected byte[] sha1;

  public LuaScript(final JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public Object call(final List<String> keys, final List<String> args) throws IOException {
    final Jedis jedis = this.jedisPool.getResource();
    try {
      return jedis.evalsha(SafeEncoder.encode(this.sha1(jedis)), keys, args);
    } finally {
      this.jedisPool.returnResource(jedis);
    }
  }

  byte[] scriptContents() throws IOException {
    if (null == this.scriptContents) {
      this.scriptContents = Resources
          .toByteArray(Resources.getResource(LuaScript.SCRIPT));
    }
    return this.scriptContents;
  }

  synchronized byte[] sha1(final Jedis jedis) throws IOException {
    if (null == this.sha1) {
      final byte[] script = this.scriptContents();
      this.sha1 = jedis.scriptLoad(script);
      this.LOGGER.info("{} ({} bytes) uploaded to redis, sha1={}",
          LuaScript.SCRIPT,
          new DecimalFormat("#,##0.#").format(script.length),
          SafeEncoder.encode(this.sha1));
    }
    return this.sha1;
  }
}
