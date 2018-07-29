/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.lib.event;

import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldVisitor;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class EventCreatorTest {

  public static class CustomEventRecordImpl implements EventRecord {

    private Field root;

    @Override
    public Header getHeader() {
      return null;
    }

    @Override
    public Field get() {
      return this.root;
    }

    @Override
    public Field set(Field field) {
      this.root = field;
      return this.root;
    }

    @Override
    public Field get(String fieldPath) {
      return null;
    }

    @Override
    public Field delete(String fieldPath) {
      return null;
    }

    @Override
    public boolean has(String fieldPath) {
      return false;
    }

    @Override
    public Set<String> getFieldPaths() {
      return null;
    }

    @Override
    public Set<String> getEscapedFieldPaths() {
      return null;
    }

    @Override
    public List<String> getEscapedFieldPathsOrdered() {
      return null;
    }

    @Override
    public Field set(String fieldPath, Field newField) {
      return null;
    }

    @Override
    public void forEachField(FieldVisitor visitor) throws StageException {
    }

    public Header setHeader(Header header) {
      return null;
    }

    @Override
    public String getEventType() {
      return null;
    }

    @Override
    public String getEventVersion() {
      return null;
    }

    @Override
    public String getEventCreationTimestamp() {
      return null;
    }
  }

  Target.Context context;
  EventCreator creator;

  @Before
  public void setUp() {
    context = Mockito.mock(Target.Context.class);
    Mockito.when(context.createEventRecord(Matchers.anyString(), Matchers.anyInt(), Matchers.anyString())).thenReturn(new CustomEventRecordImpl());

    creator = new EventCreator.Builder("custom-event", 1)
      .withRequiredField("A")
      .withOptionalField("B")
      .build();
  }

  @Test
  public void testCreateEvent() {
    // Event
    EventRecord event = creator.create(context)
      .with("A", "value")
      .with("B", "value")
      .create();

    assertNotNull(event);
    assertEquals(Field.Type.MAP, event.get().getType());
    Map<String, Field> rootMap = event.get().getValueAsMap();

    assertTrue(rootMap.containsKey("A"));
    assertTrue(rootMap.containsKey("B"));
  }

  @Test(expected = IllegalStateException.class)
  public void testMissingRequiredField() {
    creator.create(context)
      .with("B", "value")
      .create();
  }

  @Test(expected = IllegalStateException.class)
  public void testUnknownField() {
    creator.create(context)
      .with("A", "value")
      .with("Z", "value")
      .create();
  }

  @Test
  public void testWithStringMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("A", "valueA");
    map.put("B", 2);
    map.put("C", 1.0);

    // Event
    EventRecord event = creator.create(context)
      .withStringMap("A", map)
      .create();

    assertNotNull(event);
    assertEquals(Field.Type.MAP, event.get().getType());
    Map<String, Field> rootMap = event.get().getValueAsMap();

    assertTrue(rootMap.containsKey("A"));
    assertEquals(Field.Type.LIST_MAP, rootMap.get("A").getType());
    Map<String, Field> aMap = rootMap.get("A").getValueAsMap();
    assertNotNull(aMap);

    Field field;

    assertTrue(aMap.containsKey("A"));
    field = aMap.get("A");
    assertNotNull(field);
    assertEquals(Field.Type.STRING, field.getType());
    assertEquals("valueA", field.getValueAsString());

    assertTrue(aMap.containsKey("B"));
    field = aMap.get("B");
    assertNotNull(field);
    assertEquals(Field.Type.STRING, field.getType());
    assertEquals("2", field.getValueAsString());

    assertTrue(aMap.containsKey("C"));
    field = aMap.get("C");
    assertNotNull(field);
    assertEquals(Field.Type.STRING, field.getType());
    assertEquals("1.0", field.getValueAsString());
  }

}
