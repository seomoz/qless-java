package com.moz.qless.client;

import java.io.IOException;
import java.util.List;

import com.moz.qless.Client;
import com.moz.qless.QueueCounts;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class QueuesTest {
  private final JedisPool jedisPool = new JedisPool(ClientHelper.DEFAULT_HOSTNAME);
  private Client client;
  private static final String DEFAULT_NAME = "foo";

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
  public void countsBasic() {
    Assert.assertNotNull(this.client.getQueues().get(QueuesTest.DEFAULT_NAME));
  }

  @Test
  public void countsSingleJob() throws IOException {
    Assert.assertNull(this.client.getQueues().counts());
    this.client.getQueues().get(QueuesTest.DEFAULT_NAME)
        .put(QueuesTest.DEFAULT_NAME, null, null);

    final List<QueueCounts> counts = this.client.getQueues().counts();
    Assert.assertEquals(1, counts.size());

    final QueueCounts count = counts.get(0);
    Assert.assertEquals(0, count.getScheduled());
    Assert.assertEquals(QueuesTest.DEFAULT_NAME, count.getName());
    Assert.assertEquals(false, count.getPaused());
    Assert.assertEquals(1, count.getWaiting());
    Assert.assertEquals(0, count.getDepends());
    Assert.assertEquals(0, count.getDepends());
    Assert.assertEquals(0, count.getRecurring());
    Assert.assertEquals(0, count.getStalled());
  }

  @Test
  public void countsMultiJobs() throws IOException {
    Assert.assertNull(this.client.getQueues().counts());
    this.client.getQueues().get(QueuesTest.DEFAULT_NAME)
        .put(QueuesTest.DEFAULT_NAME, null, null);
    this.client.getQueues().get(QueuesTest.DEFAULT_NAME)
        .put("foo2", null, null);

    final List<QueueCounts> counts = this.client.getQueues().counts();
    Assert.assertEquals(1, counts.size());

    final QueueCounts count = counts.get(0);
    Assert.assertEquals(0, count.getScheduled());
    Assert.assertEquals(QueuesTest.DEFAULT_NAME, count.getName());
    Assert.assertEquals(false, count.getPaused());
    Assert.assertEquals(2, count.getWaiting());
    Assert.assertEquals(0, count.getDepends());
    Assert.assertEquals(0, count.getRunning());
    Assert.assertEquals(0, count.getRecurring());
    Assert.assertEquals(0, count.getStalled());
  }

  @Test
  public void countsMultiQueues() throws IOException {
    Assert.assertNull(this.client.getQueues().counts());
    this.client.getQueues().get(QueuesTest.DEFAULT_NAME)
        .put(QueuesTest.DEFAULT_NAME, null, null);
    this.client.getQueues().get("foo2")
        .put("foo2", null, null);

    final List<QueueCounts> counts = this.client.getQueues().counts();
    Assert.assertEquals(2, counts.size());
  }

  @Test
  public void countsAdvanced() throws IOException {
    Assert.assertNull(this.client.getQueues().counts());
    final String jid = this.client.getQueues().get(QueuesTest.DEFAULT_NAME)
        .put(QueuesTest.DEFAULT_NAME, null, null);

    this.client.getQueues().get(QueuesTest.DEFAULT_NAME).pop();
    List<QueueCounts> counts = this.client.getQueues().counts();
    Assert.assertEquals(1, counts.size());

    QueueCounts count = counts.get(0);
    Assert.assertEquals(0, count.getScheduled());
    Assert.assertEquals(QueuesTest.DEFAULT_NAME, count.getName());
    Assert.assertEquals(false, count.getPaused());
    Assert.assertEquals(0, count.getWaiting());
    Assert.assertEquals(0, count.getDepends());
    Assert.assertEquals(1, count.getRunning());
    Assert.assertEquals(0, count.getRecurring());
    Assert.assertEquals(0, count.getStalled());

    this.client.getJobs().get(jid).fail("group", "message");
    counts = this.client.getQueues().counts();
    Assert.assertEquals(1, counts.size());

    count = counts.get(0);
    Assert.assertEquals(0, count.getScheduled());
    Assert.assertEquals(QueuesTest.DEFAULT_NAME, count.getName());
    Assert.assertEquals(false, count.getPaused());
    Assert.assertEquals(0, count.getWaiting());
    Assert.assertEquals(0, count.getDepends());
    Assert.assertEquals(0, count.getRunning());
    Assert.assertEquals(0, count.getRecurring());
    Assert.assertEquals(0, count.getStalled());
  }
}
