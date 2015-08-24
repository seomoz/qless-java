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
import com.moz.qless.lua.LuaCommand;
import com.moz.qless.lua.LuaConfigParameter;
import com.moz.qless.utils.JsonUtils;
import org.codehaus.jackson.map.InjectableValues;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public final class Queue {

  private final Client client;
  private final String name;

  public Queue(final Client client, final String name) {
    this.client = client;
    this.name = name;
  }

  public Client getClient() {
    return client;
  }

  public QueueCounts getCounts() throws IOException {
    final Object result = this.client.call(
        LuaCommand.QUEUES,
        this.name);

    return JsonUtils.parse(result.toString(), QueueCounts.class);
  }

  private String getHeartbeatConfigName() {
    return this.name + "-" + LuaConfigParameter.HEARTBEAT;
  }

  private String getMaxConcurrencyConfigName() {
    return this.name + "-" + LuaConfigParameter.MAX_CONCURRENCY;
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

  /**
   * Returns the max concurrency for the queue, or -1 if there is none.
   */
  public int getMaxConcurrency() throws IOException {
    final String concurrency =
      (String) this.client.getConfig().get(getMaxConcurrencyConfigName());
    return concurrency == null ? -1 : Integer.parseInt(concurrency);
  }

  public String getName() {
    return this.name;
  }

  public Map<String, Object> getStats() throws IOException {
    return this.getStats(new Date());
  }

  public Map<String, Object> getStats(final Date date) throws IOException {
    final Object result = this.client.call(
        LuaCommand.STATS,
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
    try (final Jedis jedis = this.client.getJedisPool().getResource()) {
      final Transaction transaction = jedis.multi();
      transaction.zcard("ql:q:" + this.name + "-locks");
      transaction.zcard("ql:q:" + this.name + "-work");
      transaction.zcard("ql:q:" + this.name + "-scheduled");

      int length = 0;
      for (final Object obj: transaction.exec()) {
        length += (Long) obj;
      }

      return length;
    }
  }

  public void pause() throws IOException {
    this.pause(true);
  }

  public void pause(final boolean timeoutRunningJobs) throws IOException {
    this.client.call(
        LuaCommand.PAUSE,
        this.name);

    if (timeoutRunningJobs) {
    this.client.call(
        LuaCommand.TIMEOUT,
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
        LuaCommand.PEEK,
        this.name,
        count);
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
        LuaCommand.POP,
        this.name,
        this.client.workerName(),
        count);

    if (result.equals(ClientHelper.EMPTY_RESULT)) {
      return Collections.emptyList();
    }

    final InjectableValues injectables = new InjectableValues.Std().addValue("client",
        this.client);

    final JavaType javaType = new ObjectMapper().getTypeFactory()
        .constructCollectionType(ArrayList.class, Job.class);
    return JsonUtils.parse(result.toString(), javaType, injectables);
  }

  public void setHeartbeat(final int heartbeat) throws IOException {
    this.client.getConfig().put(
        this.getHeartbeatConfigName(),
        heartbeat);
  }

  /**
   * Setting to a negative number removes any max concurrency restriction.
   */
  public void setMaxConcurrency(final int maxConcurrency) throws IOException {
    if (maxConcurrency < 0) {
      this.client.getConfig().pop(getMaxConcurrencyConfigName());
    } else {
      this.client.getConfig().put(getMaxConcurrencyConfigName(), maxConcurrency);
    }
  }

  public void unpause() throws IOException {
    this.client.call(
        LuaCommand.UNPAUSE,
        this.name);
  }

  public String put(final JobSpec jobSpec) throws IOException {
    final Object result = this.client.call(
        LuaCommand.PUT,
        this.client.workerName(),
        this.name,
        jobSpec.getJid(),
        jobSpec.getKlass(),
        JsonUtils.stringify(jobSpec.getData()),
        jobSpec.getDelay(),
        "priority",
        jobSpec.getPriority(),
        "tags",
        JsonUtils.stringify(jobSpec.getTags()),
        "retries",
        jobSpec.getRetries(),
        "depends",
        JsonUtils.stringify(jobSpec.getDepends()));

    return result.toString();
  }

  public String recur(final JobSpec jobSpec) throws IOException {
    final Object result = this.client.call(
        LuaCommand.RECUR,
        this.name,
        jobSpec.getJid(),
        jobSpec.getKlass(),
        JsonUtils.stringify(jobSpec.getData()),
        LuaConfigParameter.INTERVAL,
        jobSpec.getInterval(),
        jobSpec.getDelay(),
        LuaConfigParameter.PRIORITY,
        jobSpec.getPriority(),
        LuaConfigParameter.TAGS,
        JsonUtils.stringify(jobSpec.getTags()),
        LuaConfigParameter.RETRIES,
        jobSpec.getRetries(),
        LuaConfigParameter.BACKLOG,
        jobSpec.getBacklog());

    return result.toString();
  }

}
