package com.moz.qless;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.moz.qless.client.ClientHelper;

public class JobSpec {

  private String klass;
  private Map<String, Object> data = new HashMap<>();
  private String jid = ClientHelper.generateJid();
  private int priority = ClientHelper.DEFAULT_PRIORITY;
  private int delay = ClientHelper.DEFAULT_DELAY;
  private int retries = ClientHelper.DEFAULT_RETRIES;
  private int interval = ClientHelper.DEFAULT_INTERVAL;
  private int backlog = ClientHelper.DEFAULT_BACKLOG;
  private List<String> depends = new ArrayList<>();
  private List<String> tags = new ArrayList<>();

  public static JobSpec create() {
    return new JobSpec();
  }

  public JobSpec setKlass(final String klass) {
    this.klass = klass;
    return this;
  }

  public <T> JobSpec setKlass(final Class<T> klass) {
    return setKlass(klass.getCanonicalName());
  }

  public String getKlass() {
    return klass;
  }

  public JobSpec setData(final Map<String, Object> data) {
    this.data = data;
    return this;
  }

  public JobSpec setData(final String key, final Object value) {
    this.data.put(key, value);
    return this;
  }

  public Map<String, Object> getData() {
    return data;
  }

  public JobSpec setJid(final String jid) {
    this.jid = jid;
    return this;
  }

  public String getJid() {
    return jid;
  }

  public JobSpec setPriority(final int priority) {
    this.priority = priority;
    return this;
  }

  public int getPriority() {
    return priority;
  }

  public JobSpec setRetries(final int retries) {
    this.retries = retries;
    return this;
  }

  public int getRetries() {
    return retries;
  }

  public JobSpec setDelay(final int delay) {
    this.delay = delay;
    return this;
  }

  public int getDelay() {
    return delay;
  }

  public JobSpec setInterval(final int interval) {
    this.interval = interval;
    return this;
  }

  public int getInterval() {
    return interval;
  }

  public JobSpec setBacklog(final int backlog) {
    this.backlog = backlog;
    return this;
  }

  public int getBacklog() {
    return backlog;
  }

  public JobSpec dependsOn(final String... jids) {
    return dependsOn(Arrays.asList(jids));
  }

  public JobSpec dependsOn(final List<String> jids) {
    this.depends.addAll(jids);
    return this;
  }

  public JobSpec setDepends(final String... jids) {
    return setDepends(Arrays.asList(jids));
  }

  public JobSpec setDepends(final List<String> jids) {
    this.depends = jids;
    return this;
  }

  public List<String> getDepends() {
    return depends;
  }

  public JobSpec tagged(final String... tags) {
    return tagged(Arrays.asList(tags));
  }

  public JobSpec tagged(final List<String> tags) {
    this.tags.addAll(tags);
    return this;
  }

  public JobSpec setTags(final String... tags) {
    return setTags(Arrays.asList(tags));
  }

  public JobSpec setTags(final List<String> tags) {
    this.tags = tags;
    return this;
  }

  public List<String> getTags() {
    return tags;
  }
}
