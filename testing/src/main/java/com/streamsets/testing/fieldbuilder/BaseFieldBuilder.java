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
package com.streamsets.testing.fieldbuilder;

import com.streamsets.pipeline.api.Field;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class BaseFieldBuilder<BT extends BaseFieldBuilder> {
  protected abstract BT getInstance();

  public MapFieldBuilder startMap(String name) {
    return new MapFieldBuilder(name, this);
  }

  public MapFieldBuilder startListMap(String name) {
    return startMap(name).listMap(true);
  }

  public ListFieldBuilder startList(String name) {
    return new ListFieldBuilder(name, this);
  }

  public static Map<String, String> buildAttributeMap(String... attributes) {
    if (attributes == null || attributes.length == 0) {
      return Collections.emptyMap();
    }
    List<String> list = Arrays.asList(attributes);
    Map<String, String> attrMap = IntStream.range(1, list.size())
        .filter(i -> (i + 1) % 2 == 0)
        .mapToObj(i -> new AbstractMap.SimpleEntry<>(list.get(i - 1), list.get(i)))
        .collect(Collectors.toMap(o -> o.getKey(), o -> o.getValue()));
    return attrMap;
  }

  protected abstract void handleEndChildField(String fieldName, Field fieldValue);

  public abstract BaseFieldBuilder<? extends BaseFieldBuilder> end();

  public abstract BaseFieldBuilder<? extends BaseFieldBuilder> end(String... attributes);

}
