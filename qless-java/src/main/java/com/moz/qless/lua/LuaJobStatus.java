package com.moz.qless.lua;

public enum LuaJobStatus {
  CANCELED("canceled"),
  COMPLETED("completed"),
  FAILED("failed"),
  POPPED("popped"),
  PUT("put"),
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
