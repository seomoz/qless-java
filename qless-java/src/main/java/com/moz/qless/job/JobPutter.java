package com.moz.qless.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.moz.qless.Client;
import com.moz.qless.client.ClientHelper;
import com.moz.qless.lua.LuaCommand;
import com.moz.qless.utils.JsonUtils;

public final class JobPutter {
  private final Client client;
  private final String queueName;

  private final Map<String, Object> data;
  private final String jid;
  private final int priority;
  private final int delay;
  private final int retries;
  private final List<String> depends;
  private final List<String> tags;

  private JobPutter(final Builder builder) {
    this.client = builder.client;
    this.queueName = builder.queueName;
    this.data = builder.data;
    this.jid = builder.jid;
    this.priority = builder.priority;
    this.delay = builder.delay;
    this.retries = builder.retries;
    this.depends = builder.depends;
    this.tags = builder.tags;
  }

  public String put(final String klassName) throws IOException {
    final Object result = this.client.call(
        LuaCommand.PUT,
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
    private int priority = ClientHelper.DEFAULT_PRIORITY;
    private int delay = ClientHelper.DEFAULT_DELAY;
    private int retries = ClientHelper.DEFAULT_RETRIES;
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

    public Builder priority(final int priority) {
      this.priority = priority;
      return this;
    }

    public Builder delay(final int delay) {
      this.delay = delay;
      return this;
    }

    public Builder data(final Map<String, Object> data) {
      this.data = data;
      return this;
    }

    public Builder data(final String key, final Object value) {
      this.data.put(key, value);
      return this;
    }

    public Builder retries(final int retires) {
      this.retries = retires;
      return this;
    }

    public Builder depends(final List<String> depends) {
      this.depends = depends;
      return this;
    }

    public Builder depends(final String... depends) {
      return this.depends(Arrays.asList(depends));
    }

    public Builder tags(final List<String> tags) {
      this.tags = tags;
      return this;
    }

    public Builder tags(final String... tags) {
      return this.tags(Arrays.asList(tags));
    }

    public JobPutter build() {
      return new JobPutter(this);
    }
  }
}
