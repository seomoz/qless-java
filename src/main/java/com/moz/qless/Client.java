package com.moz.qless;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.moz.qless.client.ClientHelper;
import com.moz.qless.client.Jobs;
import com.moz.qless.client.Queues;
import com.moz.qless.lua.LuaCommand;
import com.moz.qless.lua.LuaScript;
import com.moz.qless.utils.JsonUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPool;

public class Client implements AutoCloseable {
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

  private static final List<String> KEYS_LIST = new ArrayList<>();

  public Client() {
    this(ClientHelper.DEFAULT_URI);
  }

  public Client(final String uri) {
    this(URI.create(uri));
  }

  public Client(final URI uri) {
    this(new JedisPool(uri));
  }

  /**
   * Note that this constructor assumes ownership of the passed in
   * JedisPool, and it will be closed by the corresponding
   * Client#close method.
   */
  public Client(final JedisPool jedisPool) {
    this.jedisPool = jedisPool;
    this.luaScript = new LuaScript(this.jedisPool);
    this.config = new Config(this);
    this.events = new Events(this.jedisPool);
    this.jobs = new Jobs(this);
    this.queues = new Queues(this);
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

    Client.LOGGER.debug("{}", argsList);

    return this.luaScript.call(Client.KEYS_LIST, argsList);
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
    return ClientHelper.getHostName() + "-" + ClientHelper.getPid();
  }

  public Queues getQueues() {
    return this.queues;
  }

  public Queue getQueue(final String queueName) {
    return new Queue(this, queueName);
  }
}
