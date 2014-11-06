package com.moz.qless.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.moz.qless.Client;
import com.moz.qless.client.ClientHelper;
import com.moz.qless.lua.LuaCommand;
import com.moz.qless.utils.JsonUtils;

public class JobPutter {
  private final Client client;
  private final String queueName;

  private final Map<String, Object> data;
  private final String jid;
  private final String priority;
  private final String delay;
  private final String retries;
  private final List<String> depends;
  private final List<String> tags;

  public JobPutter(
      final Client client,
      final String queueName,
      final Map<String, Object> data,
      final String jid,
      final String priority,
      final String delay,
      final String retries,
      final List<String> depends,
      final List<String> tags) {
    this.client = client;
    this.queueName = queueName;
    this.data = data;
    this.jid = jid;
    this.priority = priority;
    this.delay = delay;
    this.retries = retries;
    this.depends = depends;
    this.tags = tags;
  }

  public String put(final String klassName) throws IOException {
    final Object result = this.client.call(
        LuaCommand.PUT.toString(),
        this.client.workerName(),
        this.queueName,
        this.jid,
        klassName,
        JsonUtils.stringify(this.data),
        this.delay,
        "priority",
        this.priority,
        "tags",
        JsonUtils.stringify(this.tags),
        "retries",
        this.retries,
        "depends",
        JsonUtils.stringify(this.depends));

    return result.toString();
  }

  public static class Builder {
    private final Client client;
    private final String queueName;

    private Map<String, Object> data = new HashMap<>();
    private String jid = ClientHelper.generateJid();
    private String priority = ClientHelper.DEFAULT_PRIORITY;
    private String delay = ClientHelper.DEFAULT_DELAY;
    private String retries = ClientHelper.DEFAULT_RETRIES;
    private List<String> depends = new ArrayList<>();
    private List<String> tags = new ArrayList<>();

    public Builder(final Client client, final String queueName) {
      this.client = client;
      this.queueName = queueName;
    }

    public Builder jid(final String jid) {
      this.jid = jid;
      return this;
    }

    public Builder priority(final String priority) {
      this.priority = priority;
      return this;
    }

    public Builder delay(final String delay) {
      this.delay = delay;
      return this;
    }

    public Builder data(final Map<String, Object> data) {
      this.data = data;
      return this;
    }

    public Builder retries(final String retires) {
      this.retries = retires;
      return this;
    }

    public Builder depends(final List<String> depends) {
      this.depends = depends;
      return this;
    }

    public Builder tags(final List<String> tags) {
      this.tags = tags;
      return this;
    }

    public JobPutter build() {
      return new JobPutter(
          this.client,
          this.queueName,
          this.data,
          this.jid,
          this.priority,
          this.delay,
          this.retries,
          this.depends,
          this.tags);
    }
  }
}
