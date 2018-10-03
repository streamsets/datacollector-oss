/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.event.json;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A class that represents a dynamic preview event, which includes a list of before actions (i.e. actions to be
 * run before the preview event), the preview event itself, and a list of after actions (to be run after the preview,
 * most likely for cleanup).
 *
 * This class does <b>not</b> have a corresponding DynamicPreviewEvent defined in the parent package, because it is not
 * handled directly in the normal way, by the messaging client.  Instead, instances of this event, in JSON form, will be
 * passed from Control Hub to the Data Collector (via a REST API response), which will then unpack and individually
 * handle all contained sub-events.  Therefore, there is never any automatic DTO <-> JSON conversion required.
 */
public class DynamicPreviewEventJson implements EventJson {
  private List<EventJson> beforeActions;
  private List<Integer> beforeActionsEventTypeIds;

  private PipelinePreviewEventJson previewEvent;
  private int previewEventTypeId;

  private List<EventJson> afterActions;
  private List<Integer> afterActionsEventTypeIds;

  public List<EventJson> getBeforeActions() {
    return beforeActions;
  }

  public void setBeforeActions(List<EventJson> beforeActions) {
    this.beforeActions = beforeActions;
  }

  public List<Integer> getBeforeActionsEventTypeIds() {
    return beforeActionsEventTypeIds;
  }

  public void setBeforeActionsEventTypeIds(List<Integer> beforeActionsEventTypeIds) {
    this.beforeActionsEventTypeIds = beforeActionsEventTypeIds;
  }

  public PipelinePreviewEventJson getPreviewEvent() {
    return previewEvent;
  }

  public void setPreviewEvent(PipelinePreviewEventJson previewEvent) {
    this.previewEvent = previewEvent;
  }

  public int getPreviewEventTypeId() {
    return previewEventTypeId;
  }

  public void setPreviewEventTypeId(int previewEventTypeId) {
    this.previewEventTypeId = previewEventTypeId;
  }

  public List<EventJson> getAfterActions() {
    return afterActions;
  }

  public void setAfterActions(List<EventJson> afterActions) {
    this.afterActions = afterActions;
  }

  public List<Integer> getAfterActionsEventTypeIds() {
    return afterActionsEventTypeIds;
  }

  public void setAfterActionsEventTypeIds(List<Integer> afterActionsEventTypeIds) {
    this.afterActionsEventTypeIds = afterActionsEventTypeIds;
  }

  public static class Builder {
    // keep LinkedHashSet (typeId to event) of events added in order for building
    private final Map<Integer, List<EventJson>> beforeEventTypeIdsToEvents = new LinkedHashMap<>();
    private final Map<Integer, List<EventJson>> afterEventTypeIdsToEvents = new LinkedHashMap<>();

    private PipelinePreviewEventJson previewEvent;
    private int previewEventTypeId = Integer.MIN_VALUE;

    public void setPreviewEvent(PipelinePreviewEventJson previewEvent, int previewEventTypeId) {
      this.previewEvent = previewEvent;
      this.previewEventTypeId = previewEventTypeId;
    }

    public void addBeforeAction(EventJson beforeEvent, int eventTypeId) {
      beforeEventTypeIdsToEvents.computeIfAbsent(eventTypeId, id -> new LinkedList<>());
      beforeEventTypeIdsToEvents.get(eventTypeId).add(beforeEvent);
    }

    public void addAfterAction(EventJson afterEvent, int eventTypeId) {
      afterEventTypeIdsToEvents.computeIfAbsent(eventTypeId, id -> new LinkedList<>());
      afterEventTypeIdsToEvents.get(eventTypeId).add(afterEvent);
    }

    public DynamicPreviewEventJson build() {
      final DynamicPreviewEventJson returnEvent = new DynamicPreviewEventJson();

      final LinkedList<EventJson> beforeEvents = new LinkedList<>();
      final LinkedList<Integer> beforeEventsTypeIds = new LinkedList<>();
      final LinkedList<EventJson> afterEvents = new LinkedList<>();
      final LinkedList<Integer> afterEventsTypeIds = new LinkedList<>();

      beforeEventTypeIdsToEvents.forEach((typeId, events) -> {
        events.forEach(event -> {
          beforeEvents.add(event);
          beforeEventsTypeIds.add(typeId);
        });
      });

      afterEventTypeIdsToEvents.forEach((typeId, events) -> {
        events.forEach(event -> {
          afterEvents.add(event);
          afterEventsTypeIds.add(typeId);
        });
      });

      if (previewEvent == null || previewEventTypeId == Integer.MIN_VALUE) {
        throw new IllegalStateException(
            "The preview event itself has not been set in the DynamicPreviewEventJson Builder"
        );
      }

      returnEvent.setPreviewEvent(previewEvent);
      returnEvent.setPreviewEventTypeId(previewEventTypeId);

      returnEvent.setBeforeActions(beforeEvents);
      returnEvent.setBeforeActionsEventTypeIds(beforeEventsTypeIds);

      returnEvent.setAfterActions(afterEvents);
      returnEvent.setAfterActionsEventTypeIds(afterEventsTypeIds);

      return returnEvent;
    }
  }
}
