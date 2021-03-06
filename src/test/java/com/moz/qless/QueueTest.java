package com.moz.qless;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.Test;

import com.moz.qless.client.ClientHelper;
import com.moz.qless.lua.LuaConfigParameter;

public class QueueTest extends IntegrationTest {

  @Test
  public void put() throws IOException {
    final Map<String, Object> data = new HashMap<>();
    data.put("key1", "value1");

    final String expectedJid = ClientHelper.generateJid();

    final String actualJid = this.queue.put(
      jobSpec()
        .setData(data)
        .setJid(expectedJid)
        .setRetries(3)
        .tagged("tag1", "tag2"));

    assertThat(expectedJid,
        equalTo(actualJid));
  }

  @Test
  public void jobs() throws IOException {
    this.queue.put(jobSpec());

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
    final String jid = this.queue.recur(jobSpec().setInterval(60));

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
    this.queue.put(jobSpec());

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
    assertThat(this.client.getQueue(QueueTest.DEFAULT_NAME).getHeartbeat(),
        equalTo(60));

    this.client.getQueue(QueueTest.DEFAULT_NAME).setHeartbeat(10);
    assertThat(this.client.getQueue(QueueTest.DEFAULT_NAME).getHeartbeat(),
        equalTo(10));

    assertThat(
        this.client.getConfig().get(LuaConfigParameter.HEARTBEAT).toString(),
        equalTo("60"));
  }

  @Test
  public void pop() throws IOException {
    final String jid = this.queue.put(jobSpec());

    assertThat(this.queue.pop().getJid(),
        is(jid));
    assertThat(this.queue.pop(),
        nullValue());
  }

  @Test
  public void multiPop() throws IOException {
    final String jid1 = this.queue.put(jobSpec());
    final String jid2 = this.queue.put(jobSpec());

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
    final String jid = this.queue.put(jobSpec());

    assertThat(this.queue.peek().getJid(),
        is(jid));
    assertThat(this.queue.peek().getJid(),
        is(jid));
  }

  @Test
  public void multiPeek() throws IOException {
    final String jid1 = this.queue.put(jobSpec());
    final String jid2 = this.queue.put(jobSpec());

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
    this.queue.put(jobSpec());

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
    this.queue.put(jobSpec());

    assertThat(this.queue.length(),
        equalTo(1));
  }

  @Test
  public void setMaxConcurrency() throws IOException {
    this.queue.setMaxConcurrency(10);
    assertThat(
      Integer.parseInt(
        (String) this.client.getConfig().get(this.queue.getName() + "-max-concurrency")),
      equalTo(10));
  }

  @Test
  public void getMaxConcurrency() throws IOException {
    this.client.getConfig().put(this.queue.getName() + "-max-concurrency", 10);
    assertThat(this.queue.getMaxConcurrency(), equalTo(10));
  }

  @Test
  public void getMaxConcurrencyNotSet() throws IOException {
    assertThat(this.queue.getMaxConcurrency(), equalTo(-1));
  }

  @Test
  public void unsetMaxConcurrency() throws IOException {
    this.queue.setMaxConcurrency(10);
    this.queue.setMaxConcurrency(-1);
    assertThat(this.queue.getMaxConcurrency(), equalTo(-1));
  }
}
