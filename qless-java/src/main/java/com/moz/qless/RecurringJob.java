package com.moz.qless;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.moz.qless.lua.LuaCommand;
import com.moz.qless.lua.LuaConfigParameter;
import com.moz.qless.utils.JsonUtils;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JacksonInject;

public class RecurringJob extends Job {

  @JsonProperty
  protected int backlog;

  @JsonProperty
  protected int count;

  @JsonProperty
  protected int interval;

  @JsonProperty
  protected int retries;

  @JsonCreator
  RecurringJob(@JacksonInject("client") final Client client) {
      super(client);
  }

  public int getBacklog() {
      return this.backlog;
  }

  public void backlog(final int backlog) throws IOException {
    this.client.call(
        LuaCommand.RECUR_UPDATE.toString(),
        this.jid,
        LuaConfigParameter.BACKLOG.toString(),
        Integer.toString(backlog));

    this.backlog = backlog;
  }

  @Override
  public void cancel() throws IOException {
      this.client.call(
          LuaCommand.UNRECUR.toString(),
          this.jid);
  }

  public int getCount() {
      return this.count;
  }

  public void data(final Map<String, Object> data) throws IOException {
      this.client.call(
          LuaCommand.RECUR_UPDATE.toString(),
          this.jid,
          LuaConfigParameter.DATA.toString(),
          JsonUtils.stringify(data));

      this.data = data;
  }

  @Override
  public void setDataField(final String key, final Object value) throws IOException {
    this.data.put(key,  value);
    this.data(this.data);
  }

  public int getInterval() {
      return this.interval;
  }

  public void interval(final int interval) {
      this.interval = interval;
  }

  @Override
  public boolean isRecurring() {
    return true;
  }

  public double next() {
    return this.client
        .getJedisPool()
        .getResource()
        .zscore("ql:q:" + this.queueName + "-recur", this.jid);
  }

  public void klass(final String klass) throws IOException {
      this.client.call(
          LuaCommand.RECUR_UPDATE.toString(),
          this.jid,
          LuaConfigParameter.KLASS.toString(),
          klass);

      this.klass = klass;
  }

  public String lastSpawnedJid() {
      if (this.neverSpawned()) {
          return null;
      }
      return this.jid + "-" + this.count;
  }

  public Job lastSpawnedJob() throws IOException {
      if (this.neverSpawned()) {
          return null;
      }
      return this.client.getJobs().get(this.lastSpawnedJid());
  }

  @Override
  public String move(final String queueName) throws IOException {
    this.queueName = queueName;
    this.queue = null;

    final Object result = this.client.call(
        LuaCommand.RECUR_UPDATE.toString(),
        this.jid,
        LuaConfigParameter.QUEUE.toString(),
        queueName);

    return result.toString();
  }

  public boolean neverSpawned() {
      return 0 == this.count;
  }

  @Override
  public void priority(final int priority) throws IOException {
      this.client.call(
          LuaCommand.RECUR_UPDATE.toString(),
          this.jid,
          LuaConfigParameter.PRIORITY.toString(),
          Integer.toString(priority));

      this.priority = priority;
  }

  @Override
  public void requeue(final String queueName) throws IOException {
      this.move(queueName);
  }

  @Override
  public int getRetries() {
      return this.retries;
  }

  public void retries(final int retries) throws IOException {
      this.client.call(
          LuaCommand.RECUR_UPDATE.toString(),
          this.jid,
          LuaConfigParameter.RETRIES.toString(),
          Integer.toString(retries));

      this.retries = retries;
  }

  @Override
  public void tag(final String... tags) throws IOException {
      final List<String> args = new ArrayList<String>();
      args.add(this.jid);

      for (final String tag: tags) {
          args.add(tag);
      }

      this.client.call(
          LuaCommand.RECUR_TAG.toString(),
          args);
  }

  @Override
  public void untag(final String... tags) throws IOException {
      final List<String> args = new ArrayList<String>();
      args.add(this.jid);

      for (final String tag: tags) {
          args.add(tag);
      }

      this.client.call(
          LuaCommand.RECUR_UNTAG.toString(),
          args);
  }
}
