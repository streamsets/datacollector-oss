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
package com.streamsets.pipeline.lib.el;


import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CollectionEL {

  @ElFunction(
    prefix = "collection",
    name = "contains",
    description = "Returns true if given collection contains given item."
  )
  public static boolean contains(
      @ElParam("collection") Collection collection,
      @ElParam("item") Object item
  ) {
    if(collection == null) {
      return false;
    }

    return collection.contains(item);
  }

  @ElFunction(
    prefix = "collection",
    name = "filterByRegExp",
    description = "Filter given collection by regular expression (will cast each object to string for the mapping)."
  )
  public static List filterByRegExp(
      @ElParam("collection") Collection collection,
      @ElParam("regexp") String regexp
  ) {
    if(collection == null || regexp == null) {
      return Collections.emptyList();
    }
    Pattern pattern = Pattern.compile(regexp);

    return (List)collection.stream()
        .filter(i -> pattern.matcher(i.toString()).find())
        .collect(Collectors.toList());
  }

  @ElFunction(
    prefix = "collection",
    name = "size",
    description = "Return size of the collection."
  )
  public static int size(
      @ElParam("collection") Collection collection
  ) {
    if(collection == null) {
      return 0;
    }

    return collection.size();
  }

  @ElFunction(
    prefix = "collection",
    name = "get",
    description = "Return given index from the collection."
  )
  public static Object get(
      @ElParam("collection") Collection collection,
      @ElParam("index") int index
  ) {
    if(collection == null) {
      return null;
    }

    Iterator it = collection.iterator();
    Object last = null;

    while(index >= 0 && it.hasNext()) {
      last = it.next();
      index--;
    }

    if(index == -1) {
      return last;
    } else {
      return null;
    }
  }
}
