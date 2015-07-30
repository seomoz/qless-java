package com.moz.qless;

public enum JobStatus {
  CANCELED("canceled"),
  COMPLETE("complete"),
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

  private JobStatus(final String status) {
    this.status = status;
  }

  @Override
  public String toString() {
    return this.status;
  }
}
