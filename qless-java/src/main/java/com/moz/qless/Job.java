package com.moz.qless;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import com.moz.qless.client.ClientHelper;
import com.moz.qless.lua.LuaCommand;
import com.moz.qless.lua.LuaConfigParameter;
import com.moz.qless.utils.JsonUtils;
import com.moz.qless.utils.MapDeserializer;
import com.moz.qless.utils.MapUtils;
import com.moz.qless.utils.StringArrayDeserializer;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JacksonInject;
import org.codehaus.jackson.map.annotate.JsonDeserialize;

import redis.clients.jedis.exceptions.JedisDataException;

public class Job {
  @JsonIgnore
  @JacksonInject
  protected transient Client client;

  @JsonIgnore
  protected transient Queue queue;

  @JsonProperty
  @JsonDeserialize(using = MapDeserializer.class)
  protected Map<String, Object> data;

  @JsonProperty
  @JsonDeserialize(using = StringArrayDeserializer.class)
  protected List<String> dependencies;

  @JsonProperty
  @JsonDeserialize(using = StringArrayDeserializer.class)
  protected List<String> dependents;

  @JsonProperty
  protected long expires;

  @JsonProperty
  protected Map<String, Object> failure;

  @JsonProperty
  protected List<LogHistory> history;

  @JsonProperty
  protected String jid;

  @JsonProperty(value = "klass")
  protected String klass;

  @JsonProperty
  protected int priority;

  @JsonProperty(value = "queue")
  protected String queueName;

  @JsonProperty(value = "remaining")
  protected int remaining;

  @JsonProperty(value = "retries")
  protected int originalRetries;

  @JsonProperty(value = "spawned_from_jid")
  protected String spawnedFromJid;

  @JsonProperty
  protected String state = "running";

  @JsonProperty
  @JsonDeserialize(using = StringArrayDeserializer.class)
  protected List<String> tags;

  @JsonProperty
  protected boolean tracked;

  @JsonProperty(value = "worker")
  protected String worker;

  @JsonCreator
  Job(@JacksonInject("client") final Client client) {
    this.client = client;
  }

  public void cancel() throws IOException {
    this.client.call(
        LuaCommand.CANCEL,
        this.jid);
  }

  public void complete() throws IOException {
    this.complete(null, null);
  }

  public void complete(final String nextQueueName) throws IOException {
    this.complete(nextQueueName, null);
  }

  public void complete(final String nextQueueName, final Map<String, Object> opts)
      throws IOException {
    if (null == nextQueueName) {
      this.client.call(
          LuaCommand.COMPLETE,
          this.jid,
          this.client.workerName(),
          this.queueName,
          JsonUtils.stringify(this.data));
    } else {
      this.client.call(
          LuaCommand.COMPLETE,
          this.jid,
          this.client.workerName(),
          this.queueName,
          JsonUtils.stringify(this.data),
          LuaConfigParameter.NEXT,
          nextQueueName,
          LuaConfigParameter.DELAY, MapUtils.get(opts,
              LuaConfigParameter.DELAY.toString(),
              ClientHelper.DEFAULT_DELAY.toString()),
          LuaConfigParameter.DEPENDS,
          JsonUtils.stringify(MapUtils.getList(opts,
              LuaConfigParameter.DEPENDS.toString())));
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T getDataField(final String key) {
    return (T) this.data.get(key);
  }

  @SuppressWarnings("unchecked")
  public <T> T getDataField(final Class<T> clazz, final String key) {
    return (T) this.data.get(key);
  }

  public void setDataField(final String key, final Object value) throws IOException {
    this.data.put(key, value);
  }

  public void depend(final String... jids) throws IOException {
    final List<String> args = new ArrayList<>();
    args.addAll(Arrays.asList(this.jid, "on"));
    for (final String jid : jids) {
      args.add(jid);
    }

    final String[] array = new String[args.size()];
    args.toArray(array);

    this.client.call(
        LuaCommand.DEPENDS,
        array);
  }

  public void fail(final String group, final String message) throws IOException {
    this.client.call(
        LuaCommand.FAIL,
        this.jid,
        this.client.workerName(),
        group,
        message,
        JsonUtils.stringify(this.data));
  }

  public Object failure(final String group) {
    return this.failure.get(group);
  }

  public Map<String, Object> getData() {
    return this.data;
  }

  public List<String> getDependencies() {
    return this.dependencies;
  }

  public List<String> getDependents() {
    return this.dependents;
  }

  public long getExpires() {
    return this.expires;
  }

  public Map<String, Object> getFailure() {
    return this.failure;
  }

  public List<LogHistory> getHistory() {
    return this.history;
  }

  public String getJid() {
    return this.jid;
  }

  public Class<?> getKlass() throws ClassNotFoundException {
    return Class.forName(this.klass);
  }

  public String getKlassName() {
    return this.klass;
  }

  public int getRetries() {
    return this.originalRetries;
  }

  public int getPriority() {
    return this.priority;
  }

  public Queue getQueue() {
    if (null == this.queue) {
      this.queue = new Queue(this.client, this.queueName);
    }
    return this.queue;
  }

  public String getQueueName() {
    return this.queueName;
  }

  public int getRemaining() {
    return this.remaining;
  }

  public String getSpawnedFromJid() {
    return this.spawnedFromJid;
  }

  public String getState() {
    return this.state;
  }

  public List<String> getTags() {
    return this.tags;
  }

  public boolean getTracked() {
    return this.tracked;
  }

  public long getTtl() {
    return this.expires - (System.currentTimeMillis() / 1000);
  }

  public String getWorker() {
    return this.worker;
  }

  public long heartbeat() throws IOException {
    try {
      this.expires = (Long) this.client.call(LuaCommand.HEARTBEAT, this.jid,
          this.worker, JsonUtils.stringify(this.data));
      return this.expires;
    } catch (final JedisDataException ex) {
      throw new QlessException("LostLockException", ex);
    }
  }

  public boolean isRecurring() {
    return false;
  }

  public void log(final String message) throws IOException {
    this.client.call(
        LuaCommand.LOG,
        this.jid,
        message);
  }

  public String move(final String queueName) throws IOException {
    this.queueName = queueName;
    this.queue = null;

    final Object result = getQueue().put(
      JobSpec.create()
        .setJid(this.jid)
        .setData(this.data)
        .setKlass(this.klass)
        .dependsOn(this.dependencies));

    return result.toString();
  }


  public void log(final String message, final Map<String, Object> data)
      throws IOException {
    this.client.call(
        LuaCommand.LOG,
        this.jid,
        message,
        JsonUtils.stringify(data));
  }

  public void priority(final int priority) throws IOException {
    this.client.call(
        LuaCommand.PRIORITY,
        this.jid,
        priority);
    this.priority = priority;
  }

  /**
   * Load the module containing your class, and run the appropriate method.
   * For example, if this job was popped from the queue "testing", then this
   * would invoke the "testing" method of your class.
   */
  public void process() throws IOException {
    Class<?> cls;
    try {
      cls = this.getKlass();
    } catch (final ClassNotFoundException e) {
      try {
        this.fail(this.queueName + "-" + e.getClass().getName(),
          "Failed to import " + this.klass);
      } catch (final IOException ex) {
        throw new QlessException(ex);
      }
      return;
    }

    Method method;
    try {
      method = cls.getMethod(this.queueName, Job.class);
    } catch (final NoSuchMethodException e) {
      try {
        method = cls.getMethod(ClientHelper.DEFAULT_JOB_METHOD, Job.class);
      } catch (NoSuchMethodException | SecurityException ex) {
        this.fail(this.queueName + "-" + e.getClass().getName(),
          "Method missing: " + this.klass + ":" + this.queueName);
        return;
      }
    }

    try {
      if (Modifier.isStatic(method.getModifiers())) {
        method.invoke(cls, this);
      } else {
        method.invoke(cls.newInstance(), this);
      }
    } catch (IllegalAccessException | IllegalArgumentException
        | InvocationTargetException | InstantiationException e) {
      try {
        /* Extract the stack trace. */
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        this.fail(this.queueName + "-" + e.getClass().getName(),
          sw.toString());
      } catch (final IOException ex) {
        throw new QlessException(ex);
      }
    }
  }

  public void requeue(final String queue) throws IOException {
    this.requeue(queue, null);
  }

  public void requeue(final String queueName, final Map<String, Object> opts)
      throws IOException {
    try {
    this.client.call(
        LuaCommand.REQUEUE,
        this.client.workerName(),
        queueName,
        this.jid,
        this.klass,
        JsonUtils.stringify(this.data),
        MapUtils.get(opts,
          LuaConfigParameter.DELAY.toString(),
          ClientHelper.DEFAULT_DELAY.toString()),
        LuaConfigParameter.PRIORITY,
        MapUtils.get(opts,
          LuaConfigParameter.PRIORITY.toString(),
          Integer.toString(this.priority)),
        LuaConfigParameter.TAGS,
        JsonUtils.stringify(
          MapUtils.getList(opts, LuaConfigParameter.TAGS.toString(), this.tags)),
        LuaConfigParameter.RETRIES,
        MapUtils.get(opts,
          LuaConfigParameter.RETRIES.toString(),
          ClientHelper.DEFAULT_RETRIES.toString()),
        LuaConfigParameter.DEPENDS,
        JsonUtils.stringify(MapUtils.getList(
            opts, LuaConfigParameter.DEPENDS.toString(), this.getDependencies())));
    } catch (final JedisDataException ex) {
      throw new QlessException("fail to requeue the job", ex);
    }
  }

  public void retry() throws IOException {
    try {
      this.retry(0, null, null);
    } catch (final JedisDataException ex) {
      throw new QlessException("fail to retry the job", ex);
    }
  }

  public void retry(final int delay) throws IOException {
    this.retry(delay, null, null);
  }

  public void retry(final int delay, final String group, final String message)
      throws IOException {
    if (Strings.isNullOrEmpty(group)) {
      this.client.call(
          LuaCommand.RETRY,
          this.jid,
          this.queueName,
          this.worker,
          delay);
    } else {
      this.client.call(
          LuaCommand.RETRY,
          this.jid,
          this.queueName,
          this.worker,
          delay,
          group,
          message);
    }
  }

  public void tag(final String... tags) throws IOException {
    final List<String> args = new ArrayList<>();
    args.add("add");
    args.add(this.jid);

    Collections.addAll(args, tags);

    this.client.call(
        LuaCommand.TAG,
        args);
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append(this.getClass().getName())
        .append(' ')
        .append(this.klass)
        .append(" (")
        .append(this.jid)
        .append(" / ")
        .append(this.getQueueName())
        .append(" / ")
        .append(this.state)
        .append(')')
        .toString();
  }

  public void track() throws IOException {
    this.client.call(
        LuaCommand.TRACK,
        LuaCommand.TRACK,
        this.jid);
    this.tracked = true;
  }

  public void undepend(final String... jids) throws IOException {
    final List<String> args = new ArrayList<>();
    args.add(this.jid);
    args.add("off");

    Collections.addAll(args, jids);

    final String[] array = new String[args.size()];
    args.toArray(array);

    this.client.call(
        LuaCommand.DEPENDS,
        array);
  }

  public void untag(final String... tags) throws IOException {
    final List<String> args = new ArrayList<>();
    args.add("remove");
    args.add(this.jid);

    Collections.addAll(args, tags);

    this.client.call(
        LuaCommand.TAG,
        args);
  }

  public void untrack() throws IOException {
    this.client.call(
        LuaCommand.TRACK,
        LuaCommand.UNTRACK,
        this.jid);
    this.tracked = false;
  }

  @SuppressWarnings("serial")
  public static class LogHistory extends HashMap<String, Object> {
    public String queueName() {
      return (String) this.get("q");
    }

    public Object what() {
      return this.get("what");
    }

    public Integer when() {
      return (Integer) this.get("when");
    }
  }

}
