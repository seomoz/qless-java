package com.moz.qless.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.moz.qless.client.ClientHelper;

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
    return new ObjectMapper().readerFor(type).readValue(node.toString());
  }
}
