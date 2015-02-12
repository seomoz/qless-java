package com.moz.qless.utils;

import java.io.IOException;

import com.moz.qless.client.ClientHelper;

import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.InjectableValues;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

public class JsonUtils {
  public static <T> T parse(final String json, final Class<T> klass) throws IOException {
    if (json.equals(ClientHelper.EMPTY_RESULT)) {
      return null;
    }

    return  JsonUtils.parse(json, klass, new InjectableValues.Std());
  }

  public static <T> T parse(final String json, final JavaType javaType)
      throws IOException {
    if (json.equals(ClientHelper.EMPTY_RESULT)) {
      return null;
    }

    return JsonUtils.parse(json, javaType, new InjectableValues.Std());
  }

  public static <T> T parse(final String json, final Class<T> klass,
      final InjectableValues injectables) throws IOException {
    if (json.equals(ClientHelper.EMPTY_RESULT)) {
      return null;
    }

    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    return mapper.reader(klass).withInjectableValues(injectables).readValue(json);
  }

  public static <T> T parse(final String json, final JavaType javaType,
      final InjectableValues injectables) throws IOException {
    if (json.equals(ClientHelper.EMPTY_RESULT)) {
      return null;
    }

    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    return mapper.reader(javaType).withInjectableValues(injectables).readValue(json);
  }

  public static String stringify(final Object obj) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(obj);
  }
}
