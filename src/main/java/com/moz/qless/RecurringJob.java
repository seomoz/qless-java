package com.moz.qless;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.moz.qless.lua.LuaCommand;
import com.moz.qless.lua.LuaConfigParameter;
import com.moz.qless.utils.JsonUtils;

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
  RecurringJob(@JacksonInject final Client client) {
      super(client);
  }

  public int getBacklog() {
      return this.backlog;
  }

  public void backlog(final int backlog) throws IOException {
    this.client.call(
        LuaCommand.RECUR_UPDATE,
        this.jid,
        LuaConfigParameter.BACKLOG,
        backlog);

    this.backlog = backlog;
  }

  @Override
  public void cancel() throws IOException {
      this.client.call(
          LuaCommand.UNRECUR,
          this.jid);
  }

  public int getCount() {
      return this.count;
  }

  public void data(final Map<String, Object> data) throws IOException {
      this.client.call(
          LuaCommand.RECUR_UPDATE,
          this.jid,
          LuaConfigParameter.DATA,
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
          LuaCommand.RECUR_UPDATE,
          this.jid,
          LuaConfigParameter.KLASS,
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
        LuaCommand.RECUR_UPDATE,
        this.jid,
        LuaConfigParameter.QUEUE,
        queueName);

    return result.toString();
  }

  public boolean neverSpawned() {
      return 0 == this.count;
  }

  @Override
  public void priority(final int priority) throws IOException {
      this.client.call(
          LuaCommand.RECUR_UPDATE,
          this.jid,
          LuaConfigParameter.PRIORITY,
          priority);

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
          LuaCommand.RECUR_UPDATE,
          this.jid,
          LuaConfigParameter.RETRIES,
          retries);

      this.retries = retries;
  }

  @Override
  public void tag(final String... tags) throws IOException {
      final List<String> args = new ArrayList<>();
      args.add(this.jid);

      Collections.addAll(args, tags);

      this.client.call(
          LuaCommand.RECUR_TAG,
          args);
  }

  @Override
  public void untag(final String... tags) throws IOException {
      final List<String> args = new ArrayList<>();
      args.add(this.jid);

      Collections.addAll(args, tags);

      this.client.call(
          LuaCommand.RECUR_UNTAG,
          args);
  }
}
