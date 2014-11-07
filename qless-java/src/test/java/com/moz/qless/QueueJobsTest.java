package com.moz.qless;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.moz.qless.client.ClientHelper;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

public class QueueJobsTest {
  private final JedisPool jedisPool = new JedisPool(ClientHelper.DEFAULT_HOSTNAME);
  private Client client;
  private Queue queue;
  private static final String DEFAULT_NAME = "foo";

  @Before
  public void before() throws IOException {
    this.client = ClientCreation.create(this.jedisPool);
    this.queue = new Queue(this.client, QueueJobsTest.DEFAULT_NAME);
  }

  @Test
  public void regularJobStatus() throws IOException {
    final String jid = this.queue
        .newJobPutter()
        .build()
        .put(QueueJobsTest.DEFAULT_NAME);

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

    this.queue.pop();
    assertThat(this.queue.jobs().running(),
        contains(jid));
  }

  @Test
  public void recurringJobStatus() throws IOException {
    final String jid = this.queue
        .newRecurJobPutter()
        .interval(60)
        .build()
        .recur(QueueJobsTest.DEFAULT_NAME);

    assertThat(this.queue.jobs().depends(),
        is(empty()));
    assertThat(this.queue.jobs().running(),
        is(empty()));
    assertThat(this.queue.jobs().stalled(),
        is(empty()));
    assertThat(this.queue.jobs().scheduled(),
        is(empty()));
    assertThat(this.queue.jobs().recurring(),
        contains(jid));

    this.queue.pop();
    assertThat(this.queue.jobs().running(),
        contains(jid + "-1"));
  }
}
