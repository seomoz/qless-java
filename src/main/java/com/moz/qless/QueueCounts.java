package com.moz.qless;

import com.google.common.base.Objects;
import com.moz.qless.lua.LuaJobStatus;

import org.codehaus.jackson.annotate.JsonProperty;

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
      return Objects.toStringHelper(this)
              .add("name", this.name)
              .add(LuaJobStatus.PAUSED.toString(), this.paused)
              .add(LuaJobStatus.RUNNING.toString(), this.running)
              .add(LuaJobStatus.WAITING.toString(), this.waiting)
              .add(LuaJobStatus.RECURRING.toString(), this.recurring)
              .add(LuaJobStatus.DEPENDS.toString(), this.depends)
              .add(LuaJobStatus.STALLED.toString(), this.stalled)
              .add(LuaJobStatus.SCHEDULED.toString(), this.scheduled)
              .toString();
  }
}
