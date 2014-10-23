package com.moz.qless;

import java.io.IOException;
import java.util.List;

import com.moz.qless.lua.LuaCommand;

public class QueueJobs {
  private final Client client;
  private final String name;

  public QueueJobs(final Client client, final String name) {
    this.client = client;
    this.name = name;
  }

  /**
   * Return the paginated job objects of running jobs in the queue
   */
  @SuppressWarnings("unchecked")
  public List<String> running(final int offset, final int count) throws IOException {
    final Object result = this.client.call(
        LuaCommand.JOBS.toString(),
        LuaCommand.RUNNING.toString(),
        this.name,
        Integer.toString(offset),
        Integer.toString(count));

     return (List<String>) result;
  }

  public List<String> running() throws IOException {
    return this.running(0, 25);
  }

  /**
   * Return the paginated job objects of stalled jobs in the queue
   */
  @SuppressWarnings("unchecked")
  public List<String> stalled(final int offset, final int count) throws IOException {
    final Object result = this.client.call(
        LuaCommand.JOBS.toString(),
        LuaCommand.STALLED.toString(),
        this.name,
        Integer.toString(offset),
        Integer.toString(count));

    return (List<String>) result;
  }

  public List<String> stalled() throws IOException {
    return this.stalled(0, 25);
  }

  /**
   * Return the paginated job objects of scheduled jobs in the queue
   */
  @SuppressWarnings("unchecked")
  public List<String> scheduled(final int offset, final int count) throws IOException {
    final Object result = this.client.call(
        LuaCommand.JOBS.toString(),
        LuaCommand.SCHEDULED.toString(),
        this.name,
        Integer.toString(offset),
        Integer.toString(count));

    return (List<String>) result;
  }

  public List<String> scheduled() throws IOException {
    return this.scheduled(0, 25);
  }

  /**
   * Return the paginated job objects of depends jobs in the queue
   */
  @SuppressWarnings("unchecked")
  public List<String> depends(final int offset, final int count) throws IOException {
    final Object result = this.client.call(
        LuaCommand.JOBS.toString(),
        LuaCommand.DEPENDS.toString(),
        this.name,
        Integer.toString(offset),
        Integer.toString(count));

    return (List<String>) result;
  }

  public List<String> depends() throws IOException {
    return this.depends(0, 25);
  }

  /**
   * Return the paginated job objects of recurring jobs in the queue
   */
  @SuppressWarnings("unchecked")
  public List<String> recurring(final int offset, final int count) throws IOException {
    final Object result = this.client.call(
        LuaCommand.JOBS.toString(),
        LuaCommand.RECURRING.toString(),
        this.name,
        Integer.toString(offset),
        Integer.toString(count));

    return (List<String>) result;
  }

  public List<String> recurring() throws IOException {
    return this.recurring(0, 25);
  }
}
