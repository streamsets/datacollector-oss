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
package com.streamsets.pipeline.lib.cache;

import com.google.common.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheCleaner {
  private static final Logger LOG = LoggerFactory.getLogger(CacheCleaner.class);
  private final Cache cache;
  private final long timeout;
  private final String name;
  private long lastCleanUp = 0;

  public CacheCleaner(Cache cache, String name, long timeout) {
    this.cache = cache;
    this.timeout = timeout;
    this.name = name;
  }

  public void periodicCleanUp() {
    long timeNow = System.currentTimeMillis();
    if (lastCleanUp == 0) {
      lastCleanUp = timeNow;
    } else if ((timeNow - lastCleanUp) > timeout) {
      long start = cache.stats().evictionCount();
      cache.cleanUp();
      long evicted = cache.stats().evictionCount() - start;
      if (evicted > 0) {
        LOG.debug("Cleaned up the {} cache: {} entries evicted", name, evicted);
      }
      // cleanUp takes time, so get current time again
      lastCleanUp = System.currentTimeMillis();
    }
  }
}
