package com.moz.qless;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.moz.qless.client.ClientHelper;
import com.moz.qless.lua.LuaConfigParameter;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

public class QueueTest {
  private final JedisPool jedisPool = new JedisPool(ClientHelper.DEFAULT_HOSTNAME);
  private Client client;
  private Queue queue;
  private static final String DEFAULT_NAME = "foo";

  @Before
  public void before() throws IOException {
    this.client = ClientCreation.create(this.jedisPool);
    this.queue = new Queue(this.client, QueueTest.DEFAULT_NAME);
  }

  @Test
  public void put() throws IOException {
    final Map<String, Object> data = new HashMap<>();
    data.put("key1", "value1");

    final Map<String, Object> opts = new HashMap<>();
    final String expectedJid = ClientHelper.generateJid();
    opts.put("jid", expectedJid);
    opts.put("tags", Arrays.asList("tag1", "tag2"));
    opts.put("retries", "3");

    final String actualJid = this.queue.put(QueueTest.DEFAULT_NAME, data, opts);
    assertThat(expectedJid,
        equalTo(actualJid));
  }

  @Test
  public void jobs() throws IOException {
    this.queue.put(QueueTest.DEFAULT_NAME, null, null);
    assertThat(this.queue.jobs().depends(),
        is(empty()));
    assertThat(this.queue.jobs().running(),
        is(empty()));
    assertThat(this.queue.jobs().stalled(),
        is(empty()));
    assertThat(this.queue.jobs().scheduled(),
        is(empty()));
    assertThat(this.queue.jobs().recurring(),
        is(empty()));
  }

  @Test
  public void jobsRecur() throws IOException {
    final String jid = this.queue.recur(QueueTest.DEFAULT_NAME, null, 60, null);
    assertThat(this.queue.jobs().depends(),
        is(empty()));
    assertThat(this.queue.jobs().running(),
        is(empty()));
    assertThat(this.queue.jobs().stalled(),
        is(empty()));
    assertThat(this.queue.jobs().scheduled(),
        is(empty()));
    assertThat(this.queue.jobs().recurring(),
        equalTo(Arrays.asList(jid)));
  }

  @Test
  public void counts() throws IOException {
    this.queue.put(QueueTest.DEFAULT_NAME, null, null);

    final QueueCounts count = this.queue.getCounts();
    assertThat(count.getScheduled(),
        equalTo(0));
    assertThat(count.getName(),
        equalTo(QueueTest.DEFAULT_NAME));
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
  public void heartbeat() throws IOException {
    assertThat(this.client.getQueues().get(QueueTest.DEFAULT_NAME).getHeartbeat(),
        equalTo(60));

    this.client.getQueues().get(QueueTest.DEFAULT_NAME).setHeartbeat(10);
    assertThat(this.client.getQueues().get(QueueTest.DEFAULT_NAME).getHeartbeat(),
        equalTo(10));

    assertThat(
        this.client.getConfig().get(LuaConfigParameter.HEARTBEAT).toString(),
        equalTo("60"));
  }

  @Test
  public void pop() throws IOException {
    final String jid = this.queue.put(QueueTest.DEFAULT_NAME, null, null);

    assertThat(this.queue.pop().getJid(),
        is(jid));
    assertThat(this.queue.pop(),
        nullValue());
  }

  @Test
  public void multiPop() throws IOException {
    final String jid1 = this.queue.put(QueueTest.DEFAULT_NAME, null, null);
    final String jid2 = this.queue.put(QueueTest.DEFAULT_NAME, null, null);

    final List<Job> jobs = this.queue.pop(10);
    final List<String> jids = new ArrayList<>();
    for (final Job job : jobs) {
      jids.add(job.getJid());
    }

    assertThat(jids,
        containsInAnyOrder(jid1, jid2));
    assertThat(this.queue.pop(10),
        hasSize(0));
  }

  @Test
  public void peek() throws IOException {
    final String jid = this.queue.put(QueueTest.DEFAULT_NAME, null, null);

    assertThat(this.queue.peek().getJid(),
        is(jid));
    assertThat(this.queue.peek().getJid(),
        is(jid));
  }

  @Test
  public void multiPeek() throws IOException {
    final String jid1 = this.queue.put(QueueTest.DEFAULT_NAME, null, null);
    final String jid2 = this.queue.put(QueueTest.DEFAULT_NAME, null, null);

    final List<Job> jobs = this.queue.peek(10);
    final List<String> jids = new ArrayList<>();
    for (final Job job : jobs) {
      jids.add(job.getJid());
    }

    assertThat(jids,
        containsInAnyOrder(jid1, jid2));
    assertThat(this.queue.peek(10),
        hasSize(2));
  }

  @Test
  public void stats() throws IOException {
    this.queue.put(QueueTest.DEFAULT_NAME, null, null);
    final Map<String, Object> stats = this.queue.getStats();

    assertThat(stats,
        hasKey("retries"));
    assertThat(stats,
        hasKey("wait"));
    assertThat(stats,
        hasKey("failures"));
    assertThat(stats,
        hasKey("run"));
    assertThat(stats,
        hasKey("failed"));
  }

  @Test
  public void len() throws IOException {
    this.queue.put(QueueTest.DEFAULT_NAME, null, null);
    assertThat(this.queue.length(),
        equalTo(1));
  }
}
