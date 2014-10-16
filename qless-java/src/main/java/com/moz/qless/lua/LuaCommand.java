package com.moz.qless.lua;

public enum LuaCommand {
  CANCEL("cancel"),
  COMPLETE("complete"),
  CONFIG_GET("config.get"),
  CONFIG_SET("config.set"),
  CONFIG_UNSET("config.unset"),
  DEPENDS("depends"),
  FAIL("fail"),
  FAILED("failed"),
  GET("get"),
  HEARTBEAT("heartbeat"),
  JOBS("jobs"),
  LOG("log"),
  PAUSE("pause"),
  PEEK("peek"),
  POP("pop"),
  PRIORITY("priority"),
  PUT("put"),
  QUEUES("queues"),
  RECUR("recur"),
  RECURRING("recurring"),
  RECUR_GET("recur.get"),
  RECUR_TAG("recur.tag"),
  RECUR_UNTAG("recur.untag"),
  RECUR_UPDATE("recur.update"),
  REQUEUE("requeue"),
  RETRY("retry"),
  RUNNING("running"),
  SCHEDULED("scheduled"),
  STALLED("stalled"),
  STATS("stats"),
  TAG("tag"),
  TIMEOUT("timeout"),
  TRACK("track"),
  UNFAIL("unfail"),
  UNPAUSE("unpause"),
  UNRECUR("unrecur"),
  UNTRACK("untrack"),
  WORKERS("workers");

  private final String command;

  private LuaCommand(final String command) {
    this.command = command;
  }

  @Override
  public String toString() {
    return this.command;
  }
}
