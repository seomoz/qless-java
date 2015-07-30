package com.moz.qless;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.moz.qless.client.ClientHelper;
import com.moz.qless.lua.LuaConfigParameter;

import org.junit.Assert;
import org.junit.Test;

public class JobTest extends IntegrationTest {

  @Test
  public void setPriority() throws IOException {
    final String jid = this.queue.put(jobSpec().setPriority(0));

    assertThat(this.client.getJobs().get(jid).priority,
        equalTo(0));

    this.client.getJobs().get(jid).priority(10);
    assertThat(this.client.getJobs().get(jid).priority,
        equalTo(10));
  }

  @Test
  public void queue() throws IOException {
    final String jid = this.queue.put(jobSpec());

    assertThat(this.client.getJobs().get(jid).getQueueName(),
        equalTo(JobTest.DEFAULT_NAME));
  }

  @Test
  public void klass() throws IOException, ClassNotFoundException {
    final String jid = this.queue.put(jobSpec("com.moz.qless.Job"));

    assertThat(this.client.getJobs().get(jid).getKlass().getName(),
        equalTo("com.moz.qless.Job"));
  }

  @Test
  public void klassName() throws IOException, ClassNotFoundException {
    final String jid = this.queue.put(jobSpec("com.moz.qless.JobTest"));

    assertThat(this.client.getJobs().get(jid).getKlassName(),
        equalTo("com.moz.qless.JobTest"));
  }

  @Test
  public void ttl() throws IOException {
    this.client.getConfig().put(LuaConfigParameter.HEARTBEAT.toString(), 10);
    final String jid = this.queue.put(jobSpec());

    this.queue.pop();
    Assert.assertTrue(this.client.getJobs().get(jid).getTtl() <= 10);
    Assert.assertTrue(this.client.getJobs().get(jid).getTtl() >= 9);
  }

  @Test
  public void cancel() throws IOException {
    final String jid = this.queue.put(jobSpec());

    this.client.getJobs().get(jid).cancel();

    assertThat(this.client.getJobs().get(jid),
        nullValue());
  }

  @Test(expected = QlessException.class)
  public void cancelAndRequeue() throws IOException {
    final String jid = this.queue.put(jobSpec());

    final Job job = this.client.getJobs().get(jid);

    job.cancel();
    assertThat(this.client.getJobs().get(jid),
        nullValue());

    job.requeue("bar");
  }

  @Test
  public void repr() throws IOException {
    final String jid = this.queue.put(jobSpec());

    final String str = this.client.getJobs().get(jid).toString();
    Assert.assertTrue(str.contains("foo (" + jid + " / foo / waiting)"));
  }

  @Test
  public void tagUntag() throws IOException {
    final String jid = this.queue.put(jobSpec());

    this.client.getJobs().get(jid).tag(JobTest.DEFAULT_NAME);
    assertThat(this.client.getJobs().get(jid).getTags(),
        contains(JobTest.DEFAULT_NAME));

    this.client.getJobs().get(jid).untag(JobTest.DEFAULT_NAME);
    assertThat(this.client.getJobs().get(jid).getTags(),
        is(empty()));
  }

  @Test
  public void getDataField() throws IOException {
    final String key = "foo";
    final String value = "bar";

    final String jid = this.queue.put(jobSpec().setData(key, value));

    assertThat(this.client.getJobs().get(jid).<String>getDataField(key),
        equalTo(value));
    assertThat(this.client.getJobs().get(jid).getDataField(String.class, key),
        equalTo(value));
    assertThat(this.client.getJobs().get(jid).getDataField("unknown", "DEFAULT"),
        equalTo("DEFAULT"));
  }

  @Test
  public void setDataField() throws IOException {
    final Map<String, Object> data = new HashMap<>();
    data.put(JobTest.DEFAULT_NAME, "bar1");

    final String jid = this.queue.put(jobSpec().setData(data));

    final Job job = this.client.getJobs().get(jid);
    assertThat(job.<String>getDataField(JobTest.DEFAULT_NAME),
        equalTo("bar1"));

    job.setDataField(JobTest.DEFAULT_NAME, "bar2");
    assertThat(job.<String>getDataField(JobTest.DEFAULT_NAME),
        equalTo("bar2"));
  }

  @Test
  public void addData() throws IOException {
    final String jid = this.queue.put(jobSpec().setData("hello", "world"));

    final Job job = this.client.getJobs().get(jid);
    assertThat(job.<String>getDataField("hello"), equalTo("world"));
  }

  @Test
  public void move() throws IOException {
    final String key = "key";
    final String value = "value";

    final String jid1 = this.queue.put(jobSpec().setData(key, value).dependsOn("jid2"));

    final Job job1 = this.client.getJobs().get(jid1);
    assertThat(job1.getJid(), equalTo(jid1));

    final String jid2 = this.client.getJobs().get(jid1).move("foo2");
    final Job job2 = this.client.getJobs().get(jid2);

    assertThat(jid2, equalTo(jid1));
    assertThat(job2.getQueueName(), equalTo("foo2"));
    assertThat(job2.getData(), hasKey(key));
  }

  @Test
  public void complete() throws IOException {
    final String jid = this.queue.put(jobSpec());

    this.queue.pop().complete();

    assertThat(this.client.getJobs().get(jid).getState(),
        equalTo(JobStatus.COMPLETE.toString()));
  }

  @Test
  public void advance() throws IOException {
    final String jid = this.queue.put(jobSpec());

    this.queue.pop().complete("q2");

    assertThat(this.client.getJobs().get(jid).getQueueName(),
        equalTo("q2"));
    assertThat(this.client.getJobs().get(jid).getState(),
        equalTo(JobStatus.WAITING.toString()));
  }

  @Test
  public void heartBeat() throws IOException {
    this.client.getConfig().put(LuaConfigParameter.HEARTBEAT.toString(), 10);

    this.queue.put(jobSpec());

    final Job job = this.queue.pop();
    final long before = job.getTtl();
    this.client.getConfig().put(LuaConfigParameter.HEARTBEAT.toString(), 20);
    job.heartbeat();

    Assert.assertTrue(job.getTtl() > before);
  }

  @Test(expected = QlessException.class)
  public void heartBeatFail() throws IOException {
    final String jid = this.queue.put(jobSpec());

    this.client.getJobs().get(jid).heartbeat();
  }

  @Test
  public void trackUntrack() throws IOException {
    final String jid = this.queue.put(jobSpec());

    assertThat(this.client.getJobs().get(jid).getTracked(),
        equalTo(false));

    this.client.getJobs().get(jid).track();
    assertThat(this.client.getJobs().get(jid).getTracked(),
        equalTo(true));

    this.client.getJobs().get(jid).untrack();
    assertThat(this.client.getJobs().get(jid).getTracked(),
        equalTo(false));
  }

  @Test
  public void dependUndepend() throws IOException {
    final String jidA = this.queue.put(jobSpec());

    final String jidB = this.queue.put(jobSpec());

    assertThat(this.client.getJobs().get(jidB).getDependencies(),
        is(empty()));

    final String jidC = this.queue.put(jobSpec().dependsOn(jidA));

    assertThat(this.client.getJobs().get(jidC).getDependencies(),
        contains(jidA));

    this.client.getJobs().get(jidC).depend(jidB);
    assertThat(this.client.getJobs().get(jidC).getDependencies(),
        containsInAnyOrder(jidA, jidB));

    this.client.getJobs().get(jidC).undepend(jidA);
    assertThat(this.client.getJobs().get(jidC).getDependencies(),
        not(contains(jidA)));

    this.client.getJobs().get(jidC).undepend(jidB);
    assertThat(this.client.getJobs().get(jidC).getDependencies(),
        is(empty()));
  }

  @Test(expected = QlessException.class)
  public void retryFail() throws IOException {
    final String jid = this.queue.put(jobSpec());

    this.client.getJobs().get(jid).retry();
  }

  @Test
  public void failWithoutErrorMessage() throws IOException {
    final Queue queue = this.client.getQueue("testMessagelessException");
    final String jid = queue.put(jobSpec("com.moz.qless.IntegrationTestJob"));
    queue.pop().process();
    assertThat(
      this.client.getJobs().get(jid).getState(),
      equalTo(JobStatus.FAILED.toString()));
  }

  @Test
  public void runJobBasic() throws IOException {
    final Queue queue = new Queue(this.client, "test");

    queue.put(jobSpec("com.moz.qless.IntegrationTestJob"));

    queue.pop().process();

    assertThat(IntegrationTestJob.runningHistory,
        contains("com.moz.qless.IntegrationTestJob.test"));
  }

  @Test
  public void runJobMissingKlass() throws IOException {
    final String jid = this.queue.put(jobSpec());

    this.queue.pop().process();
    assertThat(
      this.client.getJobs().get(jid).getState(),
      equalTo(JobStatus.FAILED.toString()));
  }

  @Test
  public void runJobDefaultMethod() throws IOException {
    final Queue queue = new Queue(this.client, "none");
    queue.put(jobSpec("com.moz.qless.IntegrationTestJob"));

    queue.pop().process();
    assertThat(
      IntegrationTestJob.runningHistory,
      contains("com.moz.qless.IntegrationTestJob." + ClientHelper.DEFAULT_JOB_METHOD));
  }

  public void runJobMissingMethod() throws IOException {
    final Queue queue = new Queue(this.client, "none");
    final String jid = queue.put(jobSpec("com.moz.qless.EmptyJob"));

    queue.pop().process();
    assertThat(
      this.client.getJobs().get(jid).getState(),
      equalTo(JobStatus.FAILED.toString()));
  }

  @Test
  public void history() throws IOException {
    final String jid = this.queue.put(jobSpec());

    this.client.getJobs().get(jid).log("log1");

    final Map<String, Object> data = new HashMap<>();
    data.put("key", "value");
    this.client.getJobs().get(jid).log("log2", data);

    final List<Job.LogHistory> history =
      this.client.getJobs().get(jid).getHistory();

    assertThat(history, hasSize(3));
    assertThat(history.get(1).what().toString(),
      equalTo("log1"));
    assertThat(history.get(2).get("key").toString(),
      equalTo("value"));
  }
}
