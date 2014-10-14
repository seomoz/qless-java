package com.moz.qless.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.moz.qless.client.ClientHelper;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.JavaType;

public class StringArrayDeserializer extends JsonDeserializer<List<String>> {
  @Override
  public List<String> deserialize(final JsonParser jsonParser,
      final DeserializationContext ctxt) throws IOException {
    final JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    if (node.toString().equals(ClientHelper.EMPTY_RESULT)) {
      return new ArrayList<String>();
    }

    final JavaType type = new ObjectMapper().getTypeFactory().constructCollectionType(
        List.class, String.class);
    return new ObjectMapper().reader(type).readValue(node.toString());
  }
}
