package com.moz.qless;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

import com.moz.qless.client.ClientHelper;
import com.moz.qless.client.Jobs;
import com.moz.qless.client.Queues;
import com.moz.qless.lua.LuaCommand;
import com.moz.qless.lua.LuaScript;
import com.moz.qless.utils.JsonUtils;

public final class Client implements AutoCloseable {
  private static final List<String> KEYS_LIST = new ArrayList<>();
  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

  private final JedisPool jedisPool;
  public JedisPool getJedisPool() {
    return this.jedisPool;
  }

  private final Config config;
  public Config getConfig() {
    return this.config;
  }

  private final Events events;
  public Events getEvents() {
    return this.events;
  }

  private final Jobs jobs;
  public Jobs getJobs() {
    return this.jobs;
  }

  private final LuaScript luaScript;
  private final Queues queues;
  private final String workerName;

  public static final class Builder {
    private JedisPool jedisPool;
    private String workerName;

    public Builder() {
      this.workerName = defaultWorkerName();
    }

    public Builder jedisUri(final String uri) {
      return jedisUri(URI.create(uri));
    }

    public Builder jedisUri(final URI uri) {
      resetJedis();
      jedisPool = new JedisPool(uri);
      return this;
    }

    /**
     * Note that this assumes ownership of the passed in JedisPool,
     * and it will be closed if the jedisPool is reassigned or if the
     * the eventually created Client is closed.
     */
    public Builder jedisPool(final JedisPool jedisPool) {
      resetJedis();
      this.jedisPool = jedisPool;
      return this;
    }

    public Builder workerName(final String workerName) {
      if (Strings.isNullOrEmpty(workerName)) {
        this.workerName = defaultWorkerName();
      } else {
        this.workerName = workerName;
      }
      return this;
    }

    public Client build() {
      if (jedisPool == null) {
        jedisUri(ClientHelper.DEFAULT_URI);
      }
      return new Client(jedisPool, workerName);
    }

    private void resetJedis() {
      if (jedisPool != null) {
        jedisPool.close();
        jedisPool = null;
      }
    }

    private String defaultWorkerName() {
      return ClientHelper.getHostName() + "-" + ClientHelper.getPid();
    }
  }

  /**
   * Note that this constructor assumes ownership of the passed in
   * JedisPool, and it will be closed by the corresponding
   * Client#close method.
   */
  private Client(final JedisPool jedisPool, final String workerName) {
    this.jedisPool = jedisPool;
    this.luaScript = new LuaScript(this.jedisPool);
    this.config = new Config(this);
    this.events = new Events(this.jedisPool);
    this.jobs = new Jobs(this);
    this.queues = new Queues(this);
    this.workerName = workerName;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void close() throws IOException {
    this.events.close();
    this.jedisPool.close();
  }

  public Object call(final String command, final List<Object> args) throws IOException {
    final List<String> argsList = new ArrayList<>();
    argsList.add(command);
    argsList.add(ClientHelper.getCurrentSeconds());

    for (final Object arg : args) {
      if (arg instanceof List) {
        final List<?> subArgs = (List<?>) arg;
        for (final Object subArg : subArgs) {
          argsList.add(subArg.toString());
        }
      } else {
        argsList.add(arg.toString());
      }
    }

    LOGGER.debug("{}", argsList);

    return this.luaScript.call(KEYS_LIST, argsList);
  }

  public Object call(final String command, final Object... args) throws IOException {
    return this.call(command, Arrays.asList(args));
  }

  public Object call(final LuaCommand command, final Object... args) throws IOException {
    return this.call(command.toString(), args);
  }

  public Object call(final LuaCommand command, final List<Object> args)
    throws IOException {
    return this.call(command.toString(), args);
  }

  public List<String> tags(final int offset, final int count) throws IOException {
    final List<String> params = Arrays.asList(
        "top",
        String.valueOf(offset),
        String.valueOf(count));

    final Object result = this.call(
        LuaCommand.TAG,
        params);

    final JavaType javaType = new ObjectMapper().getTypeFactory()
        .constructCollectionType(ArrayList.class, String.class);
    return JsonUtils.parse(result.toString(), javaType);
  }

  public List<String> tags() throws IOException {
    return this.tags(0, 100);
  }

  public void track(final String jid) throws IOException {
    this.call(
        LuaCommand.TRACK,
        LuaCommand.TRACK,
        jid);
  }

  public void untrack(final String jid) throws IOException {
    this.call(
        LuaCommand.TRACK,
        LuaCommand.UNTRACK,
        jid);
  }

  public void unfail(final String group, final String queue) throws IOException {
    this.unfail(group, queue, 500);
  }

  public void unfail(final String group, final String queue, final int count)
      throws IOException {
    this.call(LuaCommand.UNFAIL, queue, group, String.valueOf(count));
  }

  public String workerName() {
    return this.workerName;
  }

  public Queues getQueues() {
    return this.queues;
  }

  public Queue getQueue(final String queueName) {
    return new Queue(this, queueName);
  }
}
