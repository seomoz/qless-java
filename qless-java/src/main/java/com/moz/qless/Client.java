package com.moz.qless;

import java.io.IOException;
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

public class Client {
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

  private static final List<String> KEYS_LIST = new ArrayList<String>();

  public Client() {
    this(new JedisPool(ClientHelper.DEFAULT_HOSTNAME));
  }

  public Client(final String url) {
    this(new JedisPool(url));
  }

  public Client(final JedisPool jedisPool) {
    this.jedisPool = jedisPool;
    this.luaScript = new LuaScript(this.jedisPool);
    this.config = new Config(this);
    this.events = new Events(this.jedisPool);
    this.jobs = new Jobs(this);
    this.queues = new Queues(this);
  }

  public Object call(final String command, final List<String> args) throws IOException {
    final List<String> argsList = new ArrayList<String>();
    argsList.add(command);
    argsList.add(ClientHelper.getCurrentSeconds());

    for (final String arg : args) {
      argsList.add(arg);
    }

    Client.LOGGER.debug("{}", argsList);

    return this.luaScript.call(Client.KEYS_LIST, argsList);
  }

  public Object call(final String command, final String... args) throws IOException {
    return this.call(command, Arrays.asList(args));
  }

  public List<String> tags(final int offset, final int count) throws IOException {
    final List<String> params = Arrays.asList(
        "top",
        String.valueOf(offset),
        String.valueOf(count));

    final Object result = this.call(
        LuaCommand.TAG.toString(),
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
        LuaCommand.TRACK.toString(),
        LuaCommand.TRACK.toString(),
        jid);
  }

  public void untrack(final String jid) throws IOException {
    this.call(
        LuaCommand.TRACK.toString(),
        LuaCommand.UNTRACK.toString(),
        jid);
  }

  public void unfail(final String group, final String queue) throws IOException {
    this.unfail(group, queue, 500);
  }

  public void unfail(final String group, final String queue, final int count)
      throws IOException {
    this.call(LuaCommand.UNFAIL.toString(), queue, group, String.valueOf(count));
  }

  public String workerName() {
    return ClientHelper.getHostName() + "-" + ClientHelper.getPid();
  }

  public Queues getQueues() {
    return this.queues;
  }
}
