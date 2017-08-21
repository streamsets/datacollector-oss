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
package org.apache.commons.el;

import java.util.Collections;

import org.apache.commons.collections.map.LRUMap;
import org.slf4j.LoggerFactory;

/**
 * StreamSets specific subclass of ExpressionEvaluatorImpl that workarounds
 * memory leak in the implementation that is tracked by EL-1. This class will
 * be removed once EL-1 fix is available.
 */
@SuppressWarnings("unchecked")
public class LruExpressionEvaluatorImpl extends ExpressionEvaluatorImpl {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LruExpressionEvaluatorImpl.class);

  /**
   * Max LRU size, right now it's a magic constant.
   */
  private static final int MAX_LRU_SIZE = 5000;

  /**
   * Change the static caches to use LRU map rather then normal map.
   */
  static {
    sCachedExpressionStrings = Collections.synchronizedMap(new LRUMap(MAX_LRU_SIZE));
    sCachedExpectedTypes = new LRUMap(MAX_LRU_SIZE);
    Thread thread = new Thread("EL-cache-trimmer") {
      @Override
      public void run() {
        LOG.info(" Starting housekeeper thread for expected types cache (runs every 5mins)");
        while (true) {
          try {
            LOG.debug("Clearing expected types cache");
            sleep(5 * 60 * 1000);
            synchronized (sCachedExpectedTypes) {
              sCachedExpectedTypes.clear();
            }
          } catch (InterruptedException ex) {
          }
        }
      }
    };
    thread.setDaemon(true);
    thread.start();
  }

}
