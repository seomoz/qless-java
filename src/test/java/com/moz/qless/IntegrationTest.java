package com.moz.qless;

import java.io.IOException;

import org.junit.Before;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class IntegrationTest {
  private final JedisPool jedisPool = new JedisPool();
  protected Client client;
  protected Queue queue;
  protected static final String DEFAULT_NAME = "foo";
  protected static final String DEFAULT_JOB_CLASS_NAME =
    IntegrationTestJob.class.getName();

  @Before
  public void setupQueues() throws IOException {
    this.client = this.create();
    this.queue = new Queue(this.client, IntegrationTest.DEFAULT_NAME);
    IntegrationTestJob.runningHistory.clear();
  }

  protected JobSpec jobSpec() {
    return jobSpec(IntegrationTest.DEFAULT_NAME);
  }

  protected JobSpec jobSpec(final String klass) {
    return JobSpec.create().setKlass(klass);
  }

  private Client create() {
    try (final Jedis jedis = this.jedisPool.getResource()) {
      jedis.flushDB();
    }

    return new Client(this.jedisPool);
  }
}
