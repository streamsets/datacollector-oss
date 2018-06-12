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
package com.streamsets.pipeline.stage.processor.expression;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ELSupport {

  private ELSupport() {}

  //TODO: decide prefix. These functions seem very similar to uuid which is in DataUtilEL and has no prefix.
  @ElFunction(
    prefix = "",
    name = "emptyMap",
    description = "Creates an empty map")
  public static Map createEmptyMap() {
    return new HashMap<>();
  }

  @ElFunction(
      prefix = "",
      name = "size",
      description = "Returns the size of a map")
  public static int size(@ElParam ("map") Map map) {
    return map != null ? map.size() : -1;
  }

  @ElFunction(
      prefix = "",
      name = "isEmptyMap",
      description = "Returns true if a map is empty")
  public static boolean isEmptyMap(@ElParam ("map") Map map) {
    return map == null || map.isEmpty();
  }

  @ElFunction(
    prefix = "",
    name = "emptyList",
    description = "Creates an empty list")
  public static List createEmptyList() {
    return new ArrayList<>();
  }

  @ElFunction(
      prefix = "",
      name = "length",
      description = "Returns the length of a list")
  public static int length(@ElParam ("list") List list) {
    return list != null ? list.size() : -1;
  }

  @ElFunction(
      prefix = "",
      name = "isEmptyList",
      description = "Returns true if a list is empty")
  public static boolean isEmptyList(@ElParam ("list") List list) {
    return list == null || list.isEmpty();
  }
}
