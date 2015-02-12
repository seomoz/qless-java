package com.moz.qless.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.moz.qless.client.ClientHelper;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

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
    return new ObjectMapper().reader(type).readValue(node.asText());
  }
}
