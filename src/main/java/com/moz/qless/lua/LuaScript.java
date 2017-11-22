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
  private String encodedSha1;

  public LuaScript(final JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public Object call(final List<String> keys, final List<String> args)
      throws IOException {
    try (final Jedis jedis = this.jedisPool.getResource()) {
      return jedis.evalsha(this.calculateSha1(jedis), keys, args);
    }
  }

  private byte[] scriptContents() throws IOException {
    if (null == this.scriptContents) {
      this.scriptContents = Resources
          .toByteArray(Resources.getResource(SCRIPT));
    }

    return this.scriptContents;
  }

  private String calculateSha1(final Jedis jedis) throws IOException {
    if (null == this.encodedSha1) {
      final byte[] script = this.scriptContents();
      this.encodedSha1 = SafeEncoder.encode(jedis.scriptLoad(script));
      LOGGER.info(
          "{} ({} bytes) uploaded to redis, sha1={}",
          SCRIPT,
          new DecimalFormat("#,##0.#").format(script.length),
          this.encodedSha1);
    }

    return this.encodedSha1;
  }
}
