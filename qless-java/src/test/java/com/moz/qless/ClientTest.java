package com.moz.qless;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import com.moz.qless.client.ClientHelper;
import com.moz.qless.lua.LuaJobStatus;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

public class ClientTest {
  private final String defaultName = "foo";

  private final JedisPool jedisPool = new JedisPool(ClientHelper.DEFAULT_HOSTNAME);
  private Client client;
  private Queue queue;

  @Before
  public void before() throws IOException {
    this.client = ClientCreation.create(this.jedisPool);
    this.queue = new Queue(this.client, this.defaultName);
  }

  @Test
  public void track() throws IOException {
    final String jid = this.queue
        .newJobPutter()
        .build()
        .put(this.defaultName);

    this.client.track(jid);

    assertThat(this.client.getJobs().tracked().get(0).getJid(),
        equalTo(jid));
  }

  @Test
  public void unTrack() throws IOException {
    final String jid = this.queue
        .newJobPutter()
        .build()
        .put(this.defaultName);

    this.client.track(jid);
    assertThat(this.client.getJobs().tracked().get(0).getJid(),
        equalTo(jid));

    this.client.untrack(jid);
    assertThat(this.client.getJobs().tracked(),
        nullValue());
  }

  @Test
  @Ignore
  /*
   * This test pending on a qless-core bug filed at:
   * https://github.com/seomoz/qless-core/issues/55
   */
  public void tags() throws IOException {
    assertThat(this.client.tags(), nullValue());

    final String jid = this.queue
        .newJobPutter()
        .build()
        .put(this.defaultName);

    this.client.getJobs().get(jid).tag("tag1", "tag2");
    assertThat(this.client.tags(),
        containsInAnyOrder("tag1", "tag2"));
  }

  @Test
  public void unfail() throws IOException {
    final String jid = this.queue
        .newJobPutter()
        .build()
        .put(this.defaultName);

    this.queue.pop().fail(this.defaultName, this.defaultName);
    assertThat(this.client.getJobs().get(jid).getState(),
        equalTo(LuaJobStatus.FAILED.toString()));

    this.client.unfail(this.defaultName, this.defaultName);
    assertThat(this.client.getJobs().get(jid).getState(),
        equalTo(LuaJobStatus.WAITING.toString()));
  }
}
