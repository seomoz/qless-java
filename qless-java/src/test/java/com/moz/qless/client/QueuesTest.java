package com.moz.qless.client;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.moz.qless.Client;
import com.moz.qless.ClientCreation;
import com.moz.qless.QueueCounts;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

public class QueuesTest {
  private final JedisPool jedisPool = new JedisPool(ClientHelper.DEFAULT_HOSTNAME);
  private Client client;
  private static final String DEFAULT_NAME = "foo";

  @Before
  public void before() throws IOException {
    this.client = ClientCreation.create(this.jedisPool);
  }

  @Test
  public void countsBasic() {
    assertThat(this.client.getQueue(QueuesTest.DEFAULT_NAME),
        notNullValue());
  }

  @Test
  public void countsSingleJob() throws IOException {
    assertThat(this.client.getQueues().counts(), nullValue());
    this.client.getQueues().get(QueuesTest.DEFAULT_NAME)
        .put(QueuesTest.DEFAULT_NAME, null, null);

    final List<QueueCounts> counts = this.client.getQueues().counts();
    assertThat(counts,
        hasSize(1));

    final QueueCounts count = counts.get(0);
    assertThat(count.getScheduled(),
        equalTo(0));
    assertThat(count.getName(),
        equalTo(QueuesTest.DEFAULT_NAME));
    assertThat(count.getPaused(),
        equalTo(false));
    assertThat(count.getWaiting(),
        equalTo(1));
    assertThat(count.getDepends(),
        equalTo(0));
    assertThat(count.getRunning(),
        equalTo(0));
    assertThat(count.getRecurring(),
        equalTo(0));
    assertThat(count.getStalled(),
        equalTo(0));
  }

  @Test
  public void countsMultiJobs() throws IOException {
    assertThat(this.client.getQueues().counts(),
        nullValue());
    this.client.getQueues().get(QueuesTest.DEFAULT_NAME)
        .put(QueuesTest.DEFAULT_NAME, null, null);
    this.client.getQueues().get(QueuesTest.DEFAULT_NAME)
        .put("foo2", null, null);

    final List<QueueCounts> counts = this.client.getQueues().counts();
    assertThat(counts,
        hasSize(1));

    final QueueCounts count = counts.get(0);
    assertThat(count.getScheduled(),
        equalTo(0));
    assertThat(count.getName(),
        equalTo(QueuesTest.DEFAULT_NAME));
    assertThat(count.getPaused(),
        equalTo(false));
    assertThat(count.getWaiting(),
        equalTo(2));
    assertThat(count.getDepends(),
        equalTo(0));
    assertThat(count.getRunning(),
        equalTo(0));
    assertThat(count.getRecurring(),
        equalTo(0));
    assertThat(count.getStalled(),
        equalTo(0));
  }

  @Test
  public void countsMultiQueues() throws IOException {
    assertThat(this.client.getQueues().counts(), nullValue());
    this.client.getQueues().get(QueuesTest.DEFAULT_NAME)
        .put(QueuesTest.DEFAULT_NAME, null, null);
    this.client.getQueues().get("foo2")
        .put("foo2", null, null);

    assertThat(this.client.getQueues().counts(),
        hasSize(2));
  }

  @Test
  public void countsAdvanced() throws IOException {
    assertThat(this.client.getQueues().counts(),
        nullValue());
    final String jid = this.client.getQueues().get(QueuesTest.DEFAULT_NAME)
        .put(QueuesTest.DEFAULT_NAME, null, null);

    this.client.getQueues().get(QueuesTest.DEFAULT_NAME).pop();
    List<QueueCounts> counts = this.client.getQueues().counts();
    assertThat(counts,
        hasSize(1));

    QueueCounts count = counts.get(0);
    assertThat(count.getScheduled(),
        equalTo(0));
    assertThat(count.getName(),
        equalTo(QueuesTest.DEFAULT_NAME));
    assertThat(count.getPaused(),
        equalTo(false));
    assertThat(count.getWaiting(),
        equalTo(0));
    assertThat(count.getDepends(),
        equalTo(0));
    assertThat(count.getRunning(),
        equalTo(1));
    assertThat(count.getRecurring(),
        equalTo(0));
    assertThat(count.getStalled(),
        equalTo(0));

    this.client.getJobs().get(jid).fail("group", "message");
    counts = this.client.getQueues().counts();
    assertThat(counts,
        hasSize(1));

    count = counts.get(0);
    assertThat(count.getScheduled(),
        equalTo(0));
    assertThat(count.getName(),
        equalTo(QueuesTest.DEFAULT_NAME));
    assertThat(count.getPaused(),
        equalTo(false));
    assertThat(count.getWaiting(),
        equalTo(0));
    assertThat(count.getDepends(),
        equalTo(0));
    assertThat(count.getRunning(),
        equalTo(0));
    assertThat(count.getRecurring(),
        equalTo(0));
    assertThat(count.getStalled(),
        equalTo(0));
  }
}
