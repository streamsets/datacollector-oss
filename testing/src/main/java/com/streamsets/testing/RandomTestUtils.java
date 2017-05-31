/*
 * Copyright 2017 StreamSets Inc.
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

package com.streamsets.testing;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public abstract class RandomTestUtils {

  private RandomTestUtils() {
    //empty
  }

  private static final Random random = new Random();

  private static final Logger LOG = LoggerFactory.getLogger(RandomTestUtils.class);

  static {
    long seed;
    final String seedProp = System.getProperty("sdc.testing.random-seed");
    if (!Strings.isNullOrEmpty(seedProp)) {
      LOG.info(String.format("Loading random seed from sdc.testing.random-seed property: %s", seedProp));
      seed = Long.parseLong(seedProp);
    } else {
      seed = System.currentTimeMillis();
      LOG.info(String.format("No random seed property; using current system millis timestamp: %d", seed));
    }
    random.setSeed(seed);
  }

  /**
   * Copied from {@link org.apache.commons.lang3.RandomUtils} but using our seeded Random instance
   * @param startInclusive
   * @param endExclusive
   * @return
   */
  public static int nextInt(final int startInclusive, final int endExclusive) {
    if (startInclusive == endExclusive) {
      return startInclusive;
    }

    return startInclusive + random.nextInt(endExclusive - startInclusive);
  }

}
