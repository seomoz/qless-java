package com.moz.qless.lua;

public enum LuaConfigParameter {
  APPLICATION("application"),
  BACKLOG("backlog"),
  DATA("data"),
  DELAY("delay"),
  DEPENDS("depends"),
  GRACE_PERIOD("grace-period"),
  INTERVAL("interval"),
  JID("jid"),
  JOBS_HISTORY("jobs-history"),
  JOBS_HISTORY_COUNT("jobs-history-count"),
  KLASS("klass"),
  HEARTBEAT("heartbeat"),
  HISTOGRAM_HISTORY("histogram-history"),
  MAX_CONCURRENCY("max-concurrency"),
  NEXT("next"),
  PAUSED("paused"),
  PRIORITY("priority"),
  STATS_HISTORY("stats-history"),
  TAGS("tags"),
  OFFSET("offset"),
  RETRIES("retries"),
  QUEUE("queue");

  private final String parameter;

  private LuaConfigParameter(final String parameter) {
    this.parameter = parameter;
  }

  @Override
  public String toString() {
    return this.parameter;
  }

}
