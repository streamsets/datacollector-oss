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

  public static final String RANDOM_SEED_PROPERTY_NAME = "sdc.testing.random-seed";

  static {
    long seed;
    final String seedValue = System.getProperty(RANDOM_SEED_PROPERTY_NAME);
    if (!Strings.isNullOrEmpty(seedValue)) {
      LOG.info(String.format("Loading random seed from %s property: %s", RANDOM_SEED_PROPERTY_NAME, seedValue));
      seed = Long.parseLong(seedValue);
    } else {
      seed = System.currentTimeMillis();
      LOG.info(String.format(
          "No random seed property (%s) found; using current system millis timestamp: %d",
          RANDOM_SEED_PROPERTY_NAME,
          seed
      ));
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

  public static Random getRandom() {
    return random;
  }
}
