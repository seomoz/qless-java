package com.moz.qless.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.moz.qless.Client;
import com.moz.qless.lua.LuaCommand;
import com.moz.qless.Queue;
import com.moz.qless.QueueCounts;
import com.moz.qless.utils.JsonUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

public class Queues {
  private final Client client;

  public Queues(final Client client) {
    this.client = client;
  }

  public List<QueueCounts> counts() throws IOException {
    final Object result = this.client.call(
        LuaCommand.QUEUES.toString());

    final JavaType javaType = new ObjectMapper().getTypeFactory()
        .constructCollectionType(ArrayList.class, QueueCounts.class);
    return JsonUtils.parse(result.toString(), javaType);
  }

  public Queue get(final String queueName) {
    return new Queue(this.client, queueName);
  }
}
