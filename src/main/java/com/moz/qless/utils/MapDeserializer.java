package com.moz.qless.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.moz.qless.client.ClientHelper;

public class MapDeserializer extends JsonDeserializer<Map<String, Object>> {
  @Override
  public Map<String, Object> deserialize(final JsonParser jsonParser,
      final DeserializationContext ctxt) throws IOException {
    final JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    if (node.asText().equals(ClientHelper.EMPTY_RESULT)) {
      return new HashMap<String, Object>();
    }

    final JavaType type = new ObjectMapper().getTypeFactory().constructMapType(
        HashMap.class, String.class, Object.class);
    return new ObjectMapper().readerFor(type).readValue(node.asText());
  }
}
