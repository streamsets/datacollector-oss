/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.event;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Stage;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Event creator to allow stage developer to declare event's structure.
 */
public class EventCreator {

  /**
   * Name of the event.
   */
  private String name;

  /**
   * Current version of the event.
   */
  private int version;

  /**
   * List of required fields (have to be in all events).
   */
  private Set<String> requiredFields;

  /**
   * List of optional event - might be missing in some (or all) events.
   */
  private Set<String> requiredAndOptionalFields;

  private EventCreator(String name, int version, Set<String> requiredFields, Set<String> requiredAndOptionalFields) {
    this.name = name;
    this.version = version;
    this.requiredFields = requiredFields;
    this.requiredAndOptionalFields = requiredAndOptionalFields;
  }

  /**
   * Builder interface for creating the EventCreator instance itself.
   */
  public static class Builder {
    private String name;
    private int version;
    private ImmutableSet.Builder<String> requiredFields;
    private ImmutableSet.Builder<String> optionalFields;

    public Builder(String name, int version) {
      this.name = name;
      this.version = version;
      this.requiredFields = ImmutableSet.builder();
      this.optionalFields = ImmutableSet.builder();
    }

    public Builder withRequiredField(String field) {
      requiredFields.add(field);
      return this;
    }

    public Builder withOptionalField(String field) {
      optionalFields.add(field);
      return this;
    }

    public EventCreator build() {
      Set<String> required = requiredFields.build();
      Set<String> optional = optionalFields.build();
      Set<String> requiredAndOptional = Sets.union(required, optional);

      return new EventCreator(name, version, required, requiredAndOptional);
    }
  }

  /**
   * Create new record according to this
   */
  public EventBuilder create(Stage.Context context) {
    return new EventBuilder(context);
  }

  /**
   * Builder that is used when actually building new event based on this creator.
   */
  public class EventBuilder {

    /**
     * Context that will be used to actually generate the event record (and optionally also send it).
     */
    private Stage.Context context;

    /**
     * Map that will be used as root field of the event record.
     */
    private Map<String, Field> rootMap;

    private EventBuilder(Stage.Context context) {
      this.context = context;
      this.rootMap = new HashMap<>();
    }

    public EventBuilder with(String key, String value) {
      rootMap.put(key, Field.create(Field.Type.STRING, value));
      return this;
    }

    public EventBuilder with(String key, long value) {
      rootMap.put(key, Field.create(Field.Type.LONG, value));
      return this;
    }

    public EventBuilder with(String key, Map<String, Field> value) {
      Field field = (value instanceof LinkedHashMap)? Field.create(Field.Type.LIST_MAP, value) : Field.create(Field.Type.MAP, value);
      rootMap.put(key, field);
      return this;
    }

    public EventBuilder withStringMap(String key, Map<String, Object> value) {
      LinkedHashMap<String, Field> wrappedMap = new LinkedHashMap<>();
      for(Map.Entry<String, Object> entry : value.entrySet()) {
        wrappedMap.put(entry.getKey(), Field.create(Field.Type.STRING, entry.getValue().toString()));
      }
      rootMap.put(key, Field.create(Field.Type.LIST_MAP, wrappedMap));
      return this;
    }

    /**
     * Create the new event.
     *
     * This method will validate that all required field are present and that no "unknown" fields have been created.
     */
    public EventRecord create() {
      // Verify all required and optional fields
      Set<String> missingRequiredFields = Sets.difference(requiredFields, rootMap.keySet());
      Preconditions.checkState(missingRequiredFields.size() == 0, "Some of the required fields are missing: " + StringUtils.join(missingRequiredFields, ","));
      Set<String> unknownFields = Sets.difference(rootMap.keySet(), requiredAndOptionalFields);
      Preconditions.checkState(unknownFields.size() == 0, "There are unknown fields: " + StringUtils.join(unknownFields, ","));

      // And finally build the event itself
      EventRecord event = context.createEventRecord(name, version);
      event.set(Field.create(Field.Type.MAP, rootMap));
      return event;
    }

    /**
     * Create and send the event immediately.
     *
     * This method will validate that all required field are present and that no "unknown" fields have been created.
     */
    public void createAndSend() {
      context.toEvent(create());
    }
  }
}
