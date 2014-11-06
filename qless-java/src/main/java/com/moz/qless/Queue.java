package com.moz.qless;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.moz.qless.client.ClientHelper;
import com.moz.qless.job.JobPutter;
import com.moz.qless.job.RecurJobPutter;
import com.moz.qless.lua.LuaCommand;
import com.moz.qless.lua.LuaConfigParameter;
import com.moz.qless.utils.JsonUtils;
import com.moz.qless.utils.MapUtils;

import org.codehaus.jackson.map.InjectableValues;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class Queue {
  private final Client client;
  private final String name;

  public Queue(final Client client, final String name) {
    this.client = client;
    this.name = name;
  }

  public QueueCounts getCounts() throws IOException {
    final Object result = this.client.call(
        LuaCommand.QUEUES.toString(),
        this.name);

    return JsonUtils.parse(result.toString(), QueueCounts.class);
  }

  private String getHeartbeatConfigName() {
    return this.name + "-" + LuaConfigParameter.HEARTBEAT.toString();
  }

  public int getHeartbeat() throws IOException {
    Object heartbeat = this.client.getConfig()
        .get(this.getHeartbeatConfigName());

    if (null == heartbeat) {
      heartbeat = this.client.getConfig()
          .get(LuaConfigParameter.HEARTBEAT);
    }
    return Integer.parseInt(heartbeat.toString());
  }

  public int getMaxConcurrency() throws IOException {
    return Integer.parseInt(
        this.client.getConfig().get(LuaConfigParameter.MAX_CONCURRENCY)
        .toString());
  }

  public String getName() {
    return this.name;
  }

  public Map<String, Object> getStats() throws IOException {
    return this.getStats(new Date());
  }

  public Map<String, Object> getStats(final Date date) throws IOException {
    final Object result = this.client.call(
        LuaCommand.STATS.toString(),
        this.name,
        String.valueOf(date.getTime() / 1000));

    final JavaType javaType = new ObjectMapper().getTypeFactory().constructMapType(
        HashMap.class, String.class, Object.class);
    return JsonUtils.parse(result.toString(), javaType);
  }

  public QueueJobs jobs() {
    return new QueueJobs(this.client, this.name);
  }

  public int length() throws IOException {
    final Jedis jedis = this.client.getJedisPool().getResource();
    try {
      final Transaction transaction = jedis.multi();
      transaction.zcard("ql:q:" + this.name + "-locks");
      transaction.zcard("ql:q:" + this.name + "-work");
      transaction.zcard("ql:q:" + this.name + "-scheduled");

      int length = 0;
      for (final Object obj: transaction.exec()) {
        length += (Long) obj;
      }

      return length;
    } finally {
      this.client.getJedisPool().returnResource(jedis);
    }
  }

  public void pause() throws IOException {
    this.pause(true);
  }

  public void pause(final boolean timeoutRunningJobs) throws IOException {
    this.client.call(
        LuaCommand.PAUSE.toString(),
        this.name);

    if (timeoutRunningJobs) {
    this.client.call(
        LuaCommand.TIMEOUT.toString(),
        this.jobs().running(0, -1));
    }
  }

  public boolean paused() throws IOException {
    return this.getCounts().getPaused();
  }

  public Job peek() throws IOException {
    final List<Job> jobs = this.peek(1);
    return (null != jobs) && (!jobs.isEmpty()) ? jobs.get(0) : null;
  }

  public List<Job> peek(final int count) throws IOException {
    Preconditions.checkArgument(count > 0, "Negative job count");

    final Object result = this.client.call(
        LuaCommand.PEEK.toString(),
        this.name,
        Integer.toString(count));
    if (result.equals(ClientHelper.EMPTY_RESULT)) {
      return new ArrayList<Job>();
    }

    final InjectableValues injectables = new InjectableValues.Std().addValue(
        "client",
        this.client);

    final JavaType javaType = new ObjectMapper().getTypeFactory()
        .constructCollectionType(ArrayList.class, Job.class);
    return JsonUtils.parse(result.toString(), javaType, injectables);
  }

  public Job pop() throws IOException {
    final List<Job> jobs = this.pop(1);
    return (null != jobs) && (!jobs.isEmpty()) ? jobs.get(0) : null;
  }

  public List<Job> pop(final int count) throws IOException {
    final Object result = this.client.call(
        LuaCommand.POP.toString(),
        this.name,
        this.client.workerName(),
        Integer.toString(count));

    if (result.equals(ClientHelper.EMPTY_RESULT)) {
      return Collections.emptyList();
    }

    final InjectableValues injectables = new InjectableValues.Std().addValue("client",
        this.client);

    final JavaType javaType = new ObjectMapper().getTypeFactory()
        .constructCollectionType(ArrayList.class, Job.class);
    return JsonUtils.parse(result.toString(), javaType, injectables);
  }

  /**
   *  Either create a new job in the provided queue with the provided attributes, or move
   *  that job into that queue. If the job is being serviced by a worker, subsequent
   *  attempts by that worker to either "heartbeat" or "complete" the job should fail and
   *  return "false". The "priority" argument should be negative to be run sooner rather
   *  than later, and positive if it's less important. The "tags" argument should be a
   *  JSON array of the tags associated with the instance and the "valid after" argument
   *  should be in how many seconds the instance should be considered actionable.
   */
  public String put(final String klass, final Object data, final Map<String, Object> opts)
      throws IOException {
    final String dataJson = (data == null) ? ClientHelper.EMPTY_RESULT : JsonUtils
        .stringify(data);
    final String jid = MapUtils.get(opts, LuaConfigParameter.JID.toString(),
        ClientHelper.generateJid());
    final String priority = MapUtils.get(opts, LuaConfigParameter.PRIORITY.toString(),
        ClientHelper.DEFAULT_PRIORITY);
    final String delay = MapUtils.get(opts, LuaConfigParameter.DELAY.toString(),
        ClientHelper.DEFAULT_DELAY);
    final String retries = MapUtils.get(opts, LuaConfigParameter.RETRIES.toString(),
        ClientHelper.DEFAULT_RETRIES);
    final List<String> tags = MapUtils.getList(opts, LuaConfigParameter.TAGS.toString());
    final List<String> depends = MapUtils.getList(opts,
        LuaConfigParameter.DEPENDS.toString());

    final Object result = this.client.call(
        LuaCommand.PUT.toString(),
        this.client.workerName(),
        this.name,
        jid,
        klass,
        dataJson,
        delay,
        "priority",
        priority,
        "tags",
        JsonUtils.stringify(tags),
        "retries",
        retries,
        "depends",
        JsonUtils.stringify(depends));

    return result.toString();
  }

  public JobPutter.Builder newJobPutter() throws IOException {
    return new JobPutter.Builder(this.client, this.name);
  }

  /**
   * Place a recurring job in this queue
   */
  public String recur(final String klass, final Object data, final int interval,
      final Map<String, Object> opts) throws IOException {
    final String dataJson = (data == null) ? ClientHelper.EMPTY_RESULT : JsonUtils
        .stringify(data);
    final String jid = MapUtils.get(opts, LuaConfigParameter.JID.toString(),
        ClientHelper.generateJid());
    final String priority = MapUtils.get(opts, LuaConfigParameter.PRIORITY.toString(),
        ClientHelper.DEFAULT_PRIORITY);
    final String offset = MapUtils.get(opts, LuaConfigParameter.OFFSET.toString(),
        ClientHelper.DEFAULT_OFFSET);
    final String retries = MapUtils.get(opts, LuaConfigParameter.RETRIES.toString(),
        ClientHelper.DEFAULT_RETRIES);
    final String backlog = MapUtils.get(opts, LuaConfigParameter.BACKLOG.toString(),
        ClientHelper.DEFAULT_BACKLOG);
    final List<String> tags = MapUtils.getList(opts, LuaConfigParameter.TAGS.toString());

    final Object result = this.client.call(
        LuaCommand.RECUR.toString(),
        this.name,
        jid,
        klass,
        dataJson,
        LuaConfigParameter.INTERVAL.toString(),
        Integer.toString(interval),
        offset,
        LuaConfigParameter.PRIORITY.toString(),
        priority,
        LuaConfigParameter.TAGS.toString(),
        JsonUtils.stringify(tags),
        LuaConfigParameter.RETRIES.toString(),
        retries,
        LuaConfigParameter.BACKLOG.toString(),
        backlog);

    return result.toString();
  }

  public RecurJobPutter.Builder newRecurJobPutter() throws IOException {
    return new RecurJobPutter.Builder(this.client, this.name);
  }

  public void setHeartbeat(final int heartbeat) throws IOException {
    this.client.getConfig().put(
        this.getHeartbeatConfigName(),
        heartbeat);
  }

  public void setMaxConcurrency(final int maxConcurrency) throws IOException {
    this.client.getConfig().put(
        LuaConfigParameter.MAX_CONCURRENCY.toString(),
        maxConcurrency);
  }

  public void unpause() throws IOException {
    this.client.call(
        LuaCommand.UNPAUSE.toString(),
        this.name);
  }
}
