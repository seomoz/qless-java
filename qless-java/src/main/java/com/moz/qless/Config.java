package com.moz.qless;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.moz.qless.lua.LuaCommand;
import com.moz.qless.utils.JsonUtils;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

public class Config {
  private final Client client;

  public Config(final Client client) {
    this.client = client;
  }

  public Map<String, Object> items() throws IOException {
    final Object result = this.client.call(LuaCommand.CONFIG_GET.toString());

    final JavaType javaType = new ObjectMapper().getTypeFactory().constructMapType(
        HashMap.class, String.class, Object.class);
    return JsonUtils.parse(result.toString(), javaType);
  }

  public List<String> keys() throws IOException {
    final Map<String, Object> items = this.items();
    return new ArrayList<String>(items.keySet());
  }

  public List<Object> values() throws IOException {
    final Map<String, Object> items = this.items();
    return new ArrayList<Object>(items.values());
  }

  public Object get(final String key) throws IOException {
    return this.client.call(
        LuaCommand.CONFIG_GET.toString(),
        key);
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getAll() throws IOException {
    final Object result = this.client.call(
        LuaCommand.CONFIG_GET.toString());

    final JavaType javaType = new ObjectMapper().getTypeFactory().constructMapType(
        HashMap.class, String.class, Object.class);
    return (Map<String, Object>) JsonUtils.parse(result.toString(), javaType);
  }

  public void update(final String key, final Object value) throws IOException {
    this.client.call(
        LuaCommand.CONFIG_SET.toString(),
        key,
        value.toString());
  }

  public Object pop(final String key) throws IOException {
    final Object value = this.get(key);
    this.clear(key);

    return value;
  }

  public void clear(final String key) throws IOException {
    this.client.call(
        LuaCommand.CONFIG_UNSET.toString(),
        key);
  }

  public void clear() throws IOException {
    for (final String key : this.keys()) {
      this.clear(key);
    }
  }
}
