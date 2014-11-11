package com.moz.qless.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.moz.qless.IntegrationTest;

import org.junit.Test;

public class JobsTest extends IntegrationTest {
  @Test
  public void getSingleJob() throws IOException {
    final String expectedJid = ClientHelper.generateJid();

    assertThat(this.client.getJobs().get(expectedJid),
        nullValue());

    this.queue
        .newJobPutter()
        .jid(expectedJid)
        .build()
        .put(JobsTest.DEFAULT_NAME);

    assertThat(this.client.getJobs().get(expectedJid),
        notNullValue());
  }

  @Test
  public void getMultiJobs() throws IOException {
    final String jid1 = this.queue
        .newJobPutter()
        .build()
        .put("job1");

    final String jid2 = this.queue
        .newJobPutter()
        .build()
        .put("job2");

    final String jid3 = this.queue
        .newJobPutter()
        .build()
        .put("job3");

    assertThat(this.client.getJobs().get(jid1, jid2, jid3),
        hasSize(3));
  }

  @Test
  public void recurring() throws IOException {
    final String expectedJid = ClientHelper.generateJid();

    assertThat(this.client.getJobs().get(expectedJid),
        nullValue());

    this.queue
        .newRecurJobPutter()
        .interval(60)
        .jid(expectedJid)
        .build()
        .recur(JobsTest.DEFAULT_NAME);

    assertThat(this.client.getJobs().get(expectedJid),
        notNullValue());
  }

  @Test
  public void complete() throws IOException {
    assertThat(this.client.getJobs().complete(), is(empty()));

    final String jid1 = this.queue
        .newJobPutter()
        .build()
        .put(JobsTest.DEFAULT_NAME);

    final String jid2 = this.queue
        .newJobPutter()
        .build()
        .put(JobsTest.DEFAULT_NAME);

    assertThat(this.client.getJobs().get(jid1),
        notNullValue());
    assertThat(this.client.getJobs().get(jid2),
        notNullValue());

    this.queue.pop().complete();
    this.queue.pop().complete();

    assertThat(this.client.getJobs().complete(),
        containsInAnyOrder(jid1, jid2));
  }

  @Test
  public void tracked() throws IOException {
    assertThat(this.client.getJobs().tracked(),
        nullValue());

    final String jid1 = this.queue
        .newJobPutter()
        .build()
        .put(JobsTest.DEFAULT_NAME);

    final String jid2 = this.queue
        .newJobPutter()
        .build()
        .put(JobsTest.DEFAULT_NAME);

    assertThat(this.client.getJobs().get(jid1),
        notNullValue());
    assertThat(this.client.getJobs().get(jid2),
        notNullValue());

    this.client.track(jid1);
    this.client.track(jid2);

    assertThat(this.client.getJobs().tracked(),
        hasSize(2));

    final List<String> trackedJids = Arrays.asList(
        this.client.getJobs().tracked().get(0).getJid(),
        this.client.getJobs().tracked().get(1).getJid());
    assertThat(trackedJids,
        containsInAnyOrder(jid1, jid2));
  }

  @Test
  public void tagged() throws IOException {
    assertThat(this.client.getJobs().tagged(JobsTest.DEFAULT_NAME), nullValue());

    final String jid1 = this.queue
        .newJobPutter()
        .tags(JobsTest.DEFAULT_NAME)
        .build()
        .put(JobsTest.DEFAULT_NAME);

    final String jid2 = this.queue
        .newJobPutter()
        .tags(JobsTest.DEFAULT_NAME)
        .build()
        .put(JobsTest.DEFAULT_NAME);

    assertThat(this.client.getJobs().get(jid1),
        notNullValue());
    assertThat(this.client.getJobs().get(jid2),
        notNullValue());

    assertThat(this.client.getJobs().tagged(JobsTest.DEFAULT_NAME),
        hasSize(2));
    assertThat(this.client.getJobs().tagged(JobsTest.DEFAULT_NAME),
        containsInAnyOrder(jid1, jid2));
  }

  @Test
  public void failed() throws IOException {
    assertThat(this.client.getJobs().failed("foo"),
        nullValue());

    final String jid = this.queue
        .newJobPutter()
        .build()
        .put(JobsTest.DEFAULT_NAME);

    this.queue.pop().fail("group", "message");
    assertThat(this.client.getJobs().failed("group"),
        hasSize(1));
    assertThat(this.client.getJobs().failed("group"),
        contains(jid));
  }

  @Test
  public void failures() throws IOException {
    assertThat(null, this.client.getJobs().failed(),
        nullValue());

    this.queue
        .newJobPutter()
        .build()
        .put(JobsTest.DEFAULT_NAME);

    this.queue.pop().fail("group1", "message1");

    this.queue
        .newJobPutter()
        .build()
        .put(JobsTest.DEFAULT_NAME);

    this.queue.pop().fail("group2", "message2");

    final Map<String, Long> expected = new HashMap<String, Long>();
    expected.put("group1", (long) 1);
    expected.put("group2", (long) 1);

    assertThat(
        this.client.getJobs().failed().keySet(),
        containsInAnyOrder("group1", "group2"));
    assertThat(
        this.client.getJobs().failed().values(),
        containsInAnyOrder((long) 1, (long) 1));
  }
}
