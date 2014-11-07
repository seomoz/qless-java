package com.moz.qless.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.moz.qless.Client;
import com.moz.qless.client.ClientHelper;
import com.moz.qless.lua.LuaCommand;
import com.moz.qless.lua.LuaConfigParameter;
import com.moz.qless.utils.JsonUtils;

public class RecurJobPutter {
  private final Client client;
  private final String queueName;

  private final Map<String, Object> data;
  private final String jid;
  private final int interval;
  private final String priority;
  private final String offset;
  private final String retries;
  private final String backlog;
  private final List<String> tags;

  public RecurJobPutter(final Builder builder) {
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
  }

  public String recur(final String klassName) throws IOException {
    final Object result = this.client.call(
        LuaCommand.RECUR.toString(),
        this.queueName,
        this.jid,
        klassName,
        JsonUtils.stringify(this.data),
        LuaConfigParameter.INTERVAL.toString(),
        String.valueOf(this.interval),
        this.offset,
        LuaConfigParameter.PRIORITY.toString(),
        this.priority,
        LuaConfigParameter.TAGS.toString(),
        JsonUtils.stringify(this.tags),
        LuaConfigParameter.RETRIES.toString(),
        this.retries,
        LuaConfigParameter.BACKLOG.toString(),
        this.backlog);

    return result.toString();
  }

  public static class Builder {
    private final Client client;
    private final String queueName;

    private Map<String, Object> data = new HashMap<>();
    private String jid = ClientHelper.generateJid();
    private String priority = ClientHelper.DEFAULT_PRIORITY;
    private int interval = ClientHelper.DEFAULT_INTERVAL;
    private String offset = ClientHelper.DEFAULT_OFFSET;
    private String retries = ClientHelper.DEFAULT_RETRIES;
    private String backlog = ClientHelper.DEFAULT_BACKLOG;
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

    public Builder offset(final String offset) {
      this.offset = offset;
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

    public Builder backlog(final String backlog) {
      this.backlog = backlog;
      return this;
    }

    public Builder tags(final List<String> tags) {
      this.tags = tags;
      return this;
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
