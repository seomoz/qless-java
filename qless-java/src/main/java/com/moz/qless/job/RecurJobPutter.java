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
import com.moz.qless.lua.LuaConfigParameter;
import com.moz.qless.utils.JsonUtils;

public final class RecurJobPutter {
  private final Client client;
  private final String queueName;

  private final Map<String, Object> data;
  private final String jid;
  private final int interval;
  private final int priority;
  private final int offset;
  private final int retries;
  private final int backlog;
  private final List<String> depends;
  private final List<String> tags;

  private RecurJobPutter(final Builder builder) {
    this.client = builder.client;
    this.queueName = builder.queueName;
    this.data = builder.data;
    this.jid = builder.jid;
    this.interval = builder.interval;
    this.priority = builder.priority;
    this.offset = builder.offset;
    this.retries = builder.retries;
    this.backlog = builder.backlog;
    this.tags = builder.tags;
    this.depends = builder.depends;
  }

  public String recur(final String klassName) throws IOException {
    final Object result = this.client.call(
        LuaCommand.RECUR,
        this.queueName,
        this.jid,
        klassName,
        JsonUtils.stringify(this.data),
        LuaConfigParameter.INTERVAL,
        String.valueOf(this.interval),
        this.offset,
        LuaConfigParameter.PRIORITY,
        this.priority,
        LuaConfigParameter.TAGS,
        JsonUtils.stringify(this.tags),
        LuaConfigParameter.RETRIES,
        this.retries,
        LuaConfigParameter.BACKLOG,
        this.backlog);

    return result.toString();
  }

  public static class Builder {
    private final Client client;
    private final String queueName;

    private Map<String, Object> data = new HashMap<>();
    private String jid = ClientHelper.generateJid();
    private int priority = ClientHelper.DEFAULT_PRIORITY;
    private int interval = ClientHelper.DEFAULT_INTERVAL;
    private int offset = ClientHelper.DEFAULT_OFFSET;
    private int retries = ClientHelper.DEFAULT_RETRIES;
    private int backlog = ClientHelper.DEFAULT_BACKLOG;
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

    public Builder offset(final int offset) {
      this.offset = offset;
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

    public Builder backlog(final int backlog) {
      this.backlog = backlog;
      return this;
    }

    public Builder tags(final List<String> tags) {
      this.tags = tags;
      return this;
    }

    public Builder tags(final String... tags) {
      return this.tags(Arrays.asList(tags));
    }

    public Builder depends(final List<String> depends) {
      this.depends = depends;
      return this;
    }

    public Builder depends(final String... depends) {
      return this.depends(Arrays.asList(depends));
    }

    public Builder interval(final int interval) {
      this.interval = interval;
      return this;
    }

    public RecurJobPutter build() {
      return new RecurJobPutter(this);
    }
  }
}
