package com.moz.qless;

import java.io.IOException;
import java.util.Arrays;

import com.moz.qless.client.ClientHelper;
import com.moz.qless.lua.LuaJobStatus;

import org.junit.Assert;
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
    final String jid = this.queue.put(this.defaultName, null, null);
    this.client.track(jid);
    Assert.assertEquals(jid, this.client.getJobs().tracked().get(0).getJid());
  }

  @Test
  public void unTrack() throws IOException {
    final String jid = this.queue.put(this.defaultName, null, null);
    this.client.track(jid);
    Assert.assertEquals(jid, this.client.getJobs().tracked().get(0).getJid());

    this.client.untrack(jid);
    Assert.assertEquals(null, this.client.getJobs().tracked());
  }

  @Test
  @Ignore
  /*
   * This test pending on a qless-core bug filed at:
   * https://github.com/seomoz/qless-core/issues/55
   */
  public void tags() throws IOException {
    Assert.assertEquals(null, this.client.tags());

    final String jid = this.queue.put(this.defaultName, null, null);
    this.client.getJobs().get(jid).tag("tag1", "tag2");
    Assert.assertEquals(Arrays.asList("tag1", "tag2"), this.client.tags());
  }

  @Test
  public void unfail() throws IOException {
    final String jid = this.queue.put(this.defaultName, null, null);
    this.queue.pop().fail(this.defaultName, this.defaultName);
    Assert.assertEquals(LuaJobStatus.FAILED.toString(),
        this.client.getJobs().get(jid).getState());

    this.client.unfail(this.defaultName, this.defaultName);
    Assert.assertEquals(LuaJobStatus.WAITING.toString(),
        this.client.getJobs().get(jid).getState());
  }
}
