package com.moz.qless;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.moz.qless.client.ClientHelper;
import org.junit.Assert;
import org.junit.Test;

public class RecurringJobTest extends IntegrationTest {
  @Test
  public void recurringJobAttributes() throws IOException {
    final String jid = this.queue
        .newRecurJobPutter()
        .interval(60)
        .build()
        .recur(RecurringJobTest.DEFAULT_NAME);

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
        equalTo(ClientHelper.DEFAULT_PRIORITY));
    assertThat(job.getRetries(),
        equalTo(ClientHelper.DEFAULT_RETRIES));
  }

  @Test
  public void setPriority() throws IOException {
    final String jid = ClientHelper.generateJid();

    this.queue
        .newRecurJobPutter()
        .interval(60)
        .jid(jid)
        .priority(0)
        .build()
        .recur(RecurringJobTest.DEFAULT_NAME);

    assertThat(this.client.getJobs().get(jid).priority,
        equalTo(0));

    this.client.getJobs().get(jid).priority(10);
    assertThat(this.client.getJobs().get(jid).priority,
        equalTo(10));
  }

  @Test
  public void setRetries() throws IOException {
    final String jid = ClientHelper.generateJid();

    this.queue
        .newRecurJobPutter()
        .interval(60)
        .jid(jid)
        .retries(2)
        .build()
        .recur(RecurringJobTest.DEFAULT_NAME);

    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);
    assertThat(job.getRetries(),
        equalTo(2));

    job.retries(10);
    assertThat(this.client.getJobs().get(jid).getRetries(),
        equalTo(10));
  }

  @Test
  public void setInterval() throws IOException {
    final String jid = this.queue
        .newRecurJobPutter()
        .interval(60)
        .build()
        .recur(RecurringJobTest.DEFAULT_NAME);

    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);

    assertThat(job.getInterval(),
        equalTo(60));

    job.interval(100);
    assertThat(job.getInterval(),
        equalTo(100));
  }

  @Test
  public void setData() throws IOException {
    final String jid = this.queue
        .newRecurJobPutter()
        .interval(60)
        .build()
        .recur(RecurringJobTest.DEFAULT_NAME);

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
  public void addData() throws IOException {
    final String jid = this.queue
      .newRecurJobPutter()
      .data("hello", "world")
      .build()
      .recur(RecurringJobTest.DEFAULT_NAME);

    final Job job = this.client.getJobs().get(jid);
    assertThat(job.<String>getDataField("hello"), equalTo("world"));
  }

  @Test
  public void setKlass() throws IOException {
    final String jid = this.queue
        .newRecurJobPutter()
        .interval(60)
        .build()
        .recur(RecurringJobTest.DEFAULT_NAME);

    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);

    assertThat(job.getKlassName(),
        equalTo(RecurringJobTest.DEFAULT_NAME));

    job.klass("com.moz.qless.IntegrationTestJob");
    assertThat(job.getKlassName(),
        equalTo("com.moz.qless.IntegrationTestJob"));
  }

  @Test
  public void getNext() throws IOException {
    final String jid = this.queue
        .newRecurJobPutter()
        .interval(60)
        .build()
        .recur(RecurringJobTest.DEFAULT_NAME);

    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);

    final double next = job.next();
    this.queue.pop();

    Assert.assertTrue((job.next() - next - 60) < 1);
  }

  @Test
  public void move() throws IOException {
    final Map<String, Object> data = new HashMap<>();
    data.put("key", "value");

    final String jid = this.queue
        .newRecurJobPutter()
        .interval(60)
        .data(data)
        .depends("jid2")
        .build()
        .recur(RecurringJobTest.DEFAULT_NAME);

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
    final String jid = this.queue
        .newRecurJobPutter()
        .interval(60)
        .build()
        .recur(RecurringJobTest.DEFAULT_NAME);

    final RecurringJob job = (RecurringJob) this.client.getJobs().get(jid);

    job.cancel();
    assertThat(this.client.getJobs().get(jid), nullValue());
  }

  @Test
  public void tagUntag() throws IOException {
    final String jid = this.queue
        .newRecurJobPutter()
        .interval(60)
        .build()
        .recur(RecurringJobTest.DEFAULT_NAME);

    this.client.getJobs().get(jid).tag(RecurringJobTest.DEFAULT_NAME);
    assertThat(this.client.getJobs().get(jid).getTags(),
        contains(RecurringJobTest.DEFAULT_NAME));

    this.client.getJobs().get(jid).untag(RecurringJobTest.DEFAULT_NAME);
    assertThat(this.client.getJobs().get(jid).getTags(),
        is(empty()));
  }

}
