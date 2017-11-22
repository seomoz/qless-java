package com.moz.qless;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.moz.qless.lua.LuaCommand;
import com.moz.qless.lua.LuaConfigParameter;
import com.moz.qless.utils.JsonUtils;

public class Config {
  private final Client client;

  public Config(final Client client) {
    this.client = client;
  }

  public Map<String, Object> getMap() throws IOException {
    final Object result = this.client.call(LuaCommand.CONFIG_GET);

    final JavaType javaType = new ObjectMapper().getTypeFactory().constructMapType(
        HashMap.class, String.class, Object.class);
    return JsonUtils.parse(result.toString(), javaType);
  }

  public Set<String> keySet() throws IOException {
    final Map<String, Object> items = this.getMap();
    return items.keySet();
  }

  public List<Object> values() throws IOException {
    final Map<String, Object> items = this.getMap();
    return new ArrayList<Object>(items.values());
  }

  public Object get(final String key) throws IOException {
      return this.client.call(
          LuaCommand.CONFIG_GET,
          key);
  }

  public Object get(final LuaConfigParameter key) throws IOException {
    return this.get(key.toString());
  }

  public void put(final String key, final Object value) throws IOException {
    this.client.call(
        LuaCommand.CONFIG_SET,
        key,
        value.toString());
  }

  public void put(final LuaConfigParameter key, final Object value) throws IOException {
    this.put(key.toString(), value);
  }

  public Object pop(final String key) throws IOException {
    final Object value = this.get(key);
    this.remove(key);

    return value;
  }

  public void remove(final String key) throws IOException {
    this.client.call(
        LuaCommand.CONFIG_UNSET,
        key);
  }

  public void clear() throws IOException {
    for (final String key : this.keySet()) {
      this.remove(key);
    }
  }
}
