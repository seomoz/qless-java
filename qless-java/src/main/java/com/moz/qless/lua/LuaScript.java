package com.moz.qless.lua;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import com.google.common.io.Resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.SafeEncoder;

public class LuaScript {
  private static final Logger LOGGER = LoggerFactory.getLogger(LuaScript.class);
  private static final String SCRIPT = "qless.lua";
  private final JedisPool jedisPool;
  private byte[] scriptContents;
  private byte[] sha1;

  public LuaScript(final JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public Object call(final List<String> keys, final List<String> args)
      throws IOException {
    final Jedis jedis = this.jedisPool.getResource();
    try {
      return jedis.evalsha(SafeEncoder.encode(this.sha1(jedis)), keys, args);
    } finally {
      this.jedisPool.returnResource(jedis);
    }
  }

  private byte[] scriptContents() throws IOException {
    if (null == this.scriptContents) {
      this.scriptContents = Resources
          .toByteArray(Resources.getResource(LuaScript.SCRIPT));
    }

    return this.scriptContents;
  }

  private byte[] sha1(final Jedis jedis) throws IOException {
    if (null == this.sha1) {
      final byte[] script = this.scriptContents();
      this.sha1 = jedis.scriptLoad(script);
      LuaScript.LOGGER.info(
          "{} ({} bytes) uploaded to redis, sha1={}",
          LuaScript.SCRIPT,
          new DecimalFormat("#,##0.#").format(script.length),
          SafeEncoder.encode(this.sha1));
    }

    return this.sha1;
  }
}
