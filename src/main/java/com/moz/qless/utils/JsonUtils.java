package com.moz.qless.utils;

import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.moz.qless.client.ClientHelper;

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
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    return mapper.readerFor(klass).with(injectables).readValue(json);
  }

  public static <T> T parse(final String json, final JavaType javaType,
      final InjectableValues injectables) throws IOException {
    if (json.equals(ClientHelper.EMPTY_RESULT)) {
      return null;
    }

    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    return mapper.readerFor(javaType).with(injectables).readValue(json);
  }

  public static String stringify(final Object obj) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(obj);
  }
}
