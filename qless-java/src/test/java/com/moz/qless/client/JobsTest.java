package com.moz.qless.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.moz.qless.Client;
import com.moz.qless.ClientCreation;
import com.moz.qless.lua.LuaConfigParameter;
import com.moz.qless.Queue;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.JedisPool;

public class JobsTest {
  private final JedisPool jedisPool = new JedisPool(ClientHelper.DEFAULT_HOSTNAME);
  private Client client;
  private Queue queue;
  private static final String DEFAULT_NAME = "foo";

  @Before
  public void before() throws IOException {
    this.client = ClientCreation.create(this.jedisPool);
    this.queue = new Queue(this.client, JobsTest.DEFAULT_NAME);
  }

  @Test
  public void getSingleJob() throws IOException {
    final Map<String, Object> opts = new HashMap<>();
    final String expectedJid = ClientHelper.generateJid();
    opts.put("jid", expectedJid);

    Assert.assertNull(this.client.getJobs().get(expectedJid));
    this.queue.put(JobsTest.DEFAULT_NAME, null, opts);
    Assert.assertNotNull(this.client.getJobs().get(expectedJid));
  }

  @Test
  public void getMultiJobs() throws IOException {
    final String jid1 = this.queue.put("job1", null, null);
    final String jid2 = this.queue.put("job2", null, null);
    final String jid3 = this.queue.put("job3", null, null);

    Assert.assertTrue(3 == this.client.getJobs().get(jid1, jid2, jid3).size());
  }

  @Test
  public void recurring() throws IOException {
    final Map<String, Object> opts = new HashMap<>();
    final String expectedJid = ClientHelper.generateJid();
    opts.put("jid", expectedJid);

    Assert.assertNull(this.client.getJobs().get(expectedJid));
    this.queue.recur(JobsTest.DEFAULT_NAME, null, 60, opts);
    Assert.assertNotNull(this.client.getJobs().get(expectedJid));
  }

  @Test
  public void complete() throws IOException {
    Assert.assertEquals(new ArrayList<String>(), this.client.getJobs().complete());

    final String jid1 = this.queue.put(JobsTest.DEFAULT_NAME, null, null);
    final String jid2 = this.queue.put(JobsTest.DEFAULT_NAME, null, null);

    Assert.assertNotNull(this.client.getJobs().get(jid1));
    Assert.assertNotNull(this.client.getJobs().get(jid2));

    this.queue.pop().complete();
    this.queue.pop().complete();

    Assert.assertEquals(2, this.client.getJobs().complete().size());
    Assert.assertTrue(this.client.getJobs().complete().contains(jid1));
    Assert.assertTrue(this.client.getJobs().complete().contains(jid2));
  }

  @Test
  public void tracked() throws IOException {
    Assert.assertEquals(null, this.client.getJobs().tracked());

    final String jid1 = this.queue.put(JobsTest.DEFAULT_NAME, null, null);
    final String jid2 = this.queue.put(JobsTest.DEFAULT_NAME, null, null);

    Assert.assertNotNull(this.client.getJobs().get(jid1));
    Assert.assertNotNull(this.client.getJobs().get(jid2));

    this.client.track(jid1);
    this.client.track(jid2);

    Assert.assertEquals(2, this.client.getJobs().tracked().size());
    final List<String> trackedJids = Arrays.asList(
        this.client.getJobs().tracked().get(0).getJid(),
        this.client.getJobs().tracked().get(1).getJid());

    Assert.assertTrue(trackedJids.contains(jid1));
    Assert.assertTrue(trackedJids.contains(jid2));
  }

  @Test
  public void tagged() throws IOException {
    Assert.assertNull(this.client.getJobs().tagged(JobsTest.DEFAULT_NAME));

    final Map<String, Object> opts1 = new HashMap<>();
    opts1.put(LuaConfigParameter.TAGS.toString(), Arrays.asList(JobsTest.DEFAULT_NAME));
    final String jid1 = this.queue.put(JobsTest.DEFAULT_NAME, null, opts1);

    final Map<String, Object> opts2 = new HashMap<>();
    opts2.put(LuaConfigParameter.TAGS.toString(), Arrays.asList(JobsTest.DEFAULT_NAME));
    final String jid2 = this.queue.put(JobsTest.DEFAULT_NAME, null, opts2);

    Assert.assertNotNull(this.client.getJobs().get(jid1));
    Assert.assertNotNull(this.client.getJobs().get(jid2));

    Assert.assertEquals(2, this.client.getJobs().tagged(JobsTest.DEFAULT_NAME).size());
    Assert.assertTrue(this.client.getJobs().tagged(JobsTest.DEFAULT_NAME).contains(jid1));
    Assert.assertTrue(this.client.getJobs().tagged(JobsTest.DEFAULT_NAME).contains(jid2));
  }

  @Test
  public void failed() throws IOException {
    Assert.assertEquals(null, this.client.getJobs().failed("foo"));

    final String jid = this.queue.put(JobsTest.DEFAULT_NAME, null, null);
    this.queue.pop().fail("group", "message");
    Assert.assertEquals(1, this.client.getJobs().failed("group").size());
    Assert.assertEquals(jid, this.client.getJobs().failed("group").get(0));
  }

  @Test
  public void failures() throws IOException {
    Assert.assertEquals(null, this.client.getJobs().failed());

    this.queue.put(JobsTest.DEFAULT_NAME, null, null);
    this.queue.pop().fail("group1", "message1");

    this.queue.put(JobsTest.DEFAULT_NAME, null, null);
    this.queue.pop().fail("group2", "message2");

    final Map<String, Long> expected = new HashMap<String, Long>();
    expected.put("group1", (long) 1);
    expected.put("group2", (long) 1);

    Assert.assertEquals(expected, this.client.getJobs().failed());
  }
}
