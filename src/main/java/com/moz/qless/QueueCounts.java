package com.moz.qless;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

public class QueueCounts {
  @JsonProperty("name")
  protected String name;

  @JsonProperty("paused")
  protected boolean paused;

  @JsonProperty("running")
  protected int running;

  @JsonProperty("waiting")
  protected int waiting;

  @JsonProperty("recurring")
  protected int recurring;

  @JsonProperty("depends")
  protected int depends;

  @JsonProperty("stalled")
  protected int stalled;

  @JsonProperty("scheduled")
  protected int scheduled;

  public String getName() {
    return this.name;
  }

  public boolean getPaused() {
    return this.paused;
  }

  public int getRunning() {
    return this.running;
  }

  public int getWaiting() {
    return this.waiting;
  }

  public int getRecurring() {
    return this.recurring;
  }

  public int getDepends() {
    return this.depends;
  }

  public int getStalled() {
    return this.stalled;
  }

  public int getScheduled() {
    return this.scheduled;
  }

  @Override
  public String toString() {
    return MoreObjects
      .toStringHelper(this)
      .add("name", this.name)
      .add(JobStatus.PAUSED.toString(), this.paused)
      .add(JobStatus.RUNNING.toString(), this.running)
      .add(JobStatus.WAITING.toString(), this.waiting)
      .add(JobStatus.RECURRING.toString(), this.recurring)
      .add(JobStatus.DEPENDS.toString(), this.depends)
      .add(JobStatus.STALLED.toString(), this.stalled)
      .add(JobStatus.SCHEDULED.toString(), this.scheduled)
      .toString();
  }
}
