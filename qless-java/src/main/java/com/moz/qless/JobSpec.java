package com.moz.qless;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.moz.qless.client.ClientHelper;

public class JobSpec {

  protected String klass;
  protected Map<String, Object> data = new HashMap<>();
  protected String jid = ClientHelper.generateJid();
  protected int priority = ClientHelper.DEFAULT_PRIORITY;
  protected int delay = ClientHelper.DEFAULT_DELAY;
  protected int retries = ClientHelper.DEFAULT_RETRIES;
  protected int interval = ClientHelper.DEFAULT_INTERVAL;
  protected int backlog = ClientHelper.DEFAULT_BACKLOG;
  protected List<String> depends = new ArrayList<>();
  protected List<String> tags = new ArrayList<>();

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

  public Integer getPriority() {
    return priority;
  }

  public JobSpec setRetries(final int retries) {
    this.retries = retries;
    return this;
  }

  public Integer getRetries() {
    return retries;
  }

  public JobSpec setDelay(final int delay) {
    this.delay = delay;
    return this;
  }

  public Integer getDelay() {
    return delay;
  }

  public JobSpec setInterval(final int interval) {
    this.interval = interval;
    return this;
  }

  public Integer getInterval() {
    return interval;
  }

  public JobSpec setBacklog(final int backlog) {
    this.backlog = backlog;
    return this;
  }

  public Integer getBacklog() {
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
