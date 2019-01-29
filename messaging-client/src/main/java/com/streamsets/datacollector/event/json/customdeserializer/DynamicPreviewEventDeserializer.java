/*
 * Copyright 2019 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.datacollector.event.json.customdeserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.streamsets.datacollector.event.binding.MessagingJsonToFromDto;
import com.streamsets.datacollector.event.dto.EventType;
import com.streamsets.datacollector.event.json.DynamicPreviewEventJson;
import com.streamsets.datacollector.event.json.EventJson;
import com.streamsets.datacollector.event.json.PipelinePreviewEventJson;

import java.io.IOException;

public class DynamicPreviewEventDeserializer extends StdDeserializer<DynamicPreviewEventJson> {
  public DynamicPreviewEventDeserializer() {
    super(DynamicPreviewEventJson.class);
  }

  @Override
  public DynamicPreviewEventJson deserialize(
      JsonParser p,
      DeserializationContext ctxt
  ) throws IOException, JsonProcessingException {
    final DynamicPreviewEventJson.Builder builder = new DynamicPreviewEventJson.Builder();

    final JsonNode node = p.getCodec().readTree(p);

    final MessagingJsonToFromDto instance = MessagingJsonToFromDto.INSTANCE;

    addActionHelper(builder, false, node, instance);
    addActionHelper(builder, true, node, instance);

    final String previewEventJsonText = node.get("previewEvent").toString();
    final int previewEventTypeId = node.get("previewEventTypeId").asInt();

    final PipelinePreviewEventJson previewEventJson = instance.deserialize(
        previewEventJsonText,
        PipelinePreviewEventJson.class
    );
    builder.setPreviewEvent(previewEventJson, previewEventTypeId);

    return builder.build();
  }

  private static void addActionHelper(
      DynamicPreviewEventJson.Builder builder,
      boolean after,
      JsonNode rootNode,
      MessagingJsonToFromDto instance
  ) throws IOException {
    final String prefix = after ? "after" : "before";
    final JsonNode actionTypeIds = rootNode.get(prefix + "ActionsEventTypeIds");
    final JsonNode actions = rootNode.get(prefix + "Actions");

    for (int i = 0; i < actionTypeIds.size(); i++) {
      final int typeId = actionTypeIds.get(i).asInt();
      final String eventJsonText = actions.get(i).toString();

      final EventJson eventJson = instance.getEventJsonAndDto(
          EventType.fromValue(typeId), cls -> instance.deserialize(eventJsonText, cls)
      ).getValue();

      if (after) {
        builder.addAfterAction(eventJson, typeId);
      } else {
        builder.addBeforeAction(eventJson, typeId);
      }
    }
  }
}
