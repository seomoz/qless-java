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

public class Queue {

  private final Client client;
  private final String name;

  public Queue(final Client client, final String name) {
    this.client = client;
    this.name = name;
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

  public void setMaxConcurrency(final int maxConcurrency) throws IOException {
    this.client.getConfig().put(
        LuaConfigParameter.MAX_CONCURRENCY,
        maxConcurrency);
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
        jobSpec.jid,
        jobSpec.klass,
        JsonUtils.stringify(jobSpec.data),
        jobSpec.delay,
        "priority",
        jobSpec.priority,
        "tags",
        JsonUtils.stringify(jobSpec.tags),
        "retries",
        jobSpec.retries,
        "depends",
        JsonUtils.stringify(jobSpec.depends));

    return result.toString();
  }

  public String recur(final JobSpec jobSpec) throws IOException {
    final Object result = this.client.call(
        LuaCommand.RECUR,
        this.name,
        jobSpec.jid,
        jobSpec.klass,
        JsonUtils.stringify(jobSpec.data),
        LuaConfigParameter.INTERVAL,
        jobSpec.interval,
        jobSpec.delay,
        LuaConfigParameter.PRIORITY,
        jobSpec.priority,
        LuaConfigParameter.TAGS,
        JsonUtils.stringify(jobSpec.tags),
        LuaConfigParameter.RETRIES,
        jobSpec.retries,
        LuaConfigParameter.BACKLOG,
        jobSpec.backlog);

    return result.toString();
  }

}
