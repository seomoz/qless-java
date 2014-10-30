package com.moz.qless;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.moz.qless.client.ClientHelper;
import com.moz.qless.lua.LuaConfigParameter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

public class RecurringJobTest {
  private final JedisPool jedisPool = new JedisPool(ClientHelper.DEFAULT_HOSTNAME);
  private Client client;
  private Queue queue;
  private static final String DEFAULT_NAME = "foo";

  @Before
  public void before() throws IOException {
    this.client = ClientCreation.create(this.jedisPool);
    this.queue = new Queue(this.client, RecurringJobTest.DEFAULT_NAME);
  }

  @Test
  public void recurringJobAttributes() throws IOException {
    final String jid = this.queue.recur(RecurringJobTest.DEFAULT_NAME, null, 60, null);
    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);

    assertThat(job.getJid(),
        equalTo(jid));
    assertThat(job.getInterval(),
        equalTo(60));
    assertThat(job.getKlassName(),
        equalTo(RecurringJobTest.DEFAULT_NAME));
    assertThat(job.getQueueName(),
        equalTo(RecurringJobTest.DEFAULT_NAME));
    assertThat(job.getPriority(),
        equalTo(Integer.parseInt(ClientHelper.DEFAULT_PRIORITY)));
    assertThat(job.getRetries(),
        equalTo(Integer.parseInt(ClientHelper.DEFAULT_RETRIES)));
  }

  @Test
  public void setPriority() throws IOException {
    final Map<String, Object> opts = new HashMap<>();
    final String jid = ClientHelper.generateJid();
    opts.put("jid", jid);
    opts.put(LuaConfigParameter.PRIORITY.toString(), 0);

    this.queue.recur(RecurringJobTest.DEFAULT_NAME, null, 60, opts);
    assertThat(this.client.getJobs().get(jid).priority,
        equalTo(0));

    this.client.getJobs().get(jid).priority(10);
    assertThat(this.client.getJobs().get(jid).priority,
        equalTo(10));
  }

  @Test
  public void setRetries() throws IOException {
    final Map<String, Object> opts = new HashMap<>();
    final String jid = ClientHelper.generateJid();
    opts.put("jid", jid);
    opts.put(LuaConfigParameter.RETRIES.toString(), 2);

    this.queue.recur(RecurringJobTest.DEFAULT_NAME, null, 60, opts);
    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);
    assertThat(job.getRetries(),
        equalTo(2));

    job.retries(10);
    assertThat(this.client.getJobs().get(jid).getRetries(),
        equalTo(10));
  }

  @Test
  public void setInterval() throws IOException {
    final String jid = this.queue.recur(RecurringJobTest.DEFAULT_NAME, null, 60, null);
    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);

    assertThat(job.getInterval(),
        equalTo(60));

    job.interval(100);
    assertThat(job.getInterval(),
        equalTo(100));
  }

  @Test
  public void setData() throws IOException {
    final String jid = this.queue.recur(RecurringJobTest.DEFAULT_NAME, null, 60, null);
    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);

    assertThat(job.getData().keySet(),
        is(empty()));

    final Map<String, Object> data = new HashMap<>();
    data.put("key1", "value1");
    data.put("key2", "value2");
    job.data(data);

    assertThat(job.getData().keySet(),
        containsInAnyOrder("key1", "key2"));
  }

  @Test
  public void setKlass() throws IOException {
    final String jid = this.queue.recur(RecurringJobTest.DEFAULT_NAME, null, 60, null);
    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);

    assertThat(job.getKlassName(),
        equalTo(RecurringJobTest.DEFAULT_NAME));

    job.klass("com.moz.qless.IntegrationTestJob");
    assertThat(job.getKlassName(),
        equalTo("com.moz.qless.IntegrationTestJob"));
  }

  @Test
  public void getNext() throws IOException {
    final String jid = this.queue.recur(RecurringJobTest.DEFAULT_NAME, null, 60, null);
    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);

    final double next = job.next();
    this.queue.pop();

    Assert.assertTrue((job.next() - next - 60) < 1);
  }

  @Test
  public void move() throws IOException {
    final Map<String, Object> data = new HashMap<>();
    data.put("key", "value");

    final Map<String, Object> opts = new HashMap<>();
    opts.put(LuaConfigParameter.DEPENDS.toString(), Arrays.asList("jid2"));

    final String jid = this.queue.recur(RecurringJobTest.DEFAULT_NAME, data, 60, opts);
    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);
    assertThat(job.getJid(),
        equalTo(jid));

    job.move("bar");

    assertThat(this.client.getJobs().get(jid).getQueueName(),
        equalTo("bar"));
    assertThat(this.client.getJobs().get(jid).getData().keySet(),
        contains("key"));
  }

  @Test
  public void cancel() throws IOException {
    final String jid = this.queue.recur(RecurringJobTest.DEFAULT_NAME, null, 60, null);
    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);

    job.cancel();
    assertThat(this.client.getJobs().get(jid), nullValue());
  }

  @Test
  public void tagUntag() throws IOException {
    final String jid = this.queue.recur(RecurringJobTest.DEFAULT_NAME, null, 60, null);

    this.client.getJobs().get(jid).tag(RecurringJobTest.DEFAULT_NAME);
    assertThat(this.client.getJobs().get(jid).getTags(),
        contains(RecurringJobTest.DEFAULT_NAME));

    this.client.getJobs().get(jid).untag(RecurringJobTest.DEFAULT_NAME);
    assertThat(this.client.getJobs().get(jid).getTags(),
        is(empty()));
  }

}
