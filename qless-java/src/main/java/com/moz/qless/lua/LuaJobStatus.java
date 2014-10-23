package com.moz.qless.lua;

public enum LuaJobStatus {
  CANCELED("canceled"),
  COMPLETED("completed"),
  DEPENDS("depends"),
  FAILED("failed"),
  PAUSED("paused"),
  POPPED("popped"),
  PUT("put"),
  RECURRING("recurring"),
  RUNNING("running"),
  SCHEDULED("scheduled"),
  STALLED("stalled"),
  TRACK("track"),
  UNTRACK("untrack"),
  WAITING("waiting");

  private final String status;

  private LuaJobStatus(final String status) {
    this.status = status;
  }

  @Override
  public String toString() {
    return this.status;
  }
}
