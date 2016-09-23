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
package org.apache.commons.el;

import java.util.Collections;

import org.apache.commons.collections.map.LRUMap;

/**
 * StreamSets specific subclass of ExpressionEvaluatorImpl that workarounds
 * memory leak in the implementation that is tracked by EL-1. This class will
 * be removed once EL-1 fix is available.
 */
public class LruExpressionEvaluatorImpl extends ExpressionEvaluatorImpl {
  /**
   * Max LRU size, right now it's a magic constant.
   */
  private static int MAX_LRU_SIZE = 5000;

  /**
   * Change the static caches to use LRU map rather then normal map.
   */
  static {
    sCachedExpressionStrings = Collections.synchronizedMap(new LRUMap(MAX_LRU_SIZE));
    sCachedExpectedTypes = new LRUMap(MAX_LRU_SIZE);
  }

}
