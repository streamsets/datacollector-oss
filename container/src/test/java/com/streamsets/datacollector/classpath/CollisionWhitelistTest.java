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
package com.streamsets.datacollector.classpath;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CollisionWhitelistTest {

  private final static Dependency NETTY_2_9_8 = new Dependency("netty", "2.9.8");
  private final static Dependency NETTY_3_1_2 = new Dependency("netty", "3.1.2");
  private final static Dependency NETTY_3_5_3 = new Dependency("netty", "3.5.3");
  private final static Dependency NETTY_4_8_0 = new Dependency("netty", "4.8.0");

  private final static Dependency JACKSON_1_9_13 = new Dependency("jackson", "1.9.13");
  private final static Dependency JACKSON_2_8_0_ANN = new Dependency("jackson-annotations-2.8.0.jar", "jackson", "2.8.0");
  private final static Dependency JACKSON_2_8_0 = new Dependency("jackson-xc-2.8.0.jar", "jackson", "2.8.0");
  private final static Dependency JACKSON_2_8_9 = new Dependency("jackson-xc-2.8.9.jar", "jackson", "2.8.9");
  private final static Dependency JACKSON_2_3_5 = new Dependency("jackson-xc-2.3.5.jar", "jackson", "2.3.5");

  private final static Dependency COOL_2_9_8 = new Dependency("cool", "2.9.8");
  private final static Dependency COOL_3_1_2 = new Dependency("cool", "3.1.2");
  private final static Dependency COOL_4_4_4 = new Dependency("cool", "4.4.4");

  @Test
  public void testNetty() {
    // Two different major versions should be whitelisted
    assertTrue(CollisionWhitelist.isWhitelisted("netty", null, ImmutableMap.of(
      "3.1.2", ImmutableList.of(NETTY_3_1_2),
      "4.8.0", ImmutableList.of(NETTY_4_8_0)
    )));

    // Different minor on the same major should be an error
    assertFalse(CollisionWhitelist.isWhitelisted("netty", null, ImmutableMap.of(
      "3.1.2", ImmutableList.of(NETTY_3_1_2),
      "3.5.3", ImmutableList.of(NETTY_3_5_3)
    )));

    // Major "2" is not whitelisted at all
    assertFalse(CollisionWhitelist.isWhitelisted("netty", null, ImmutableMap.of(
      "2.9.8", ImmutableList.of(NETTY_2_9_8),
      "3.5.3", ImmutableList.of(NETTY_3_5_3)
    )));

    // Two different minor are wrong regardless how many major versions are whitelisted
    assertFalse(CollisionWhitelist.isWhitelisted("netty", null, ImmutableMap.of(
      "3.1.2", ImmutableList.of(NETTY_3_1_2),
      "3.5.3", ImmutableList.of(NETTY_3_5_3),
      "4.8.0", ImmutableList.of(NETTY_4_8_0)
    )));
  }

  @Test
  public void testJackson() {
    // Normal, but unusual state
    assertTrue(CollisionWhitelist.isWhitelisted("jackson", null, ImmutableMap.of(
      "2.8.9", ImmutableList.of(JACKSON_2_8_9)
    )));

    // Another normal "major" state - 1.x and 2.x can be on the classpath at the same time
    assertTrue(CollisionWhitelist.isWhitelisted("jackson", null, ImmutableMap.of(
      "1.9.13", ImmutableList.of(JACKSON_1_9_13),
      "2.8.9", ImmutableList.of(JACKSON_2_8_9)
    )));

    // Correct Jackson weirdness - 2.8.9 anything depends on 2.8.0 annotations
    assertTrue(CollisionWhitelist.isWhitelisted("jackson", null, ImmutableMap.of(
      "2.8.0", ImmutableList.of(JACKSON_2_8_0_ANN),
      "2.8.9", ImmutableList.of(JACKSON_2_8_9)
    )));

    // Incorrect - only ANN can be 2.8.0 and rest on a different dot-dot version
    assertFalse(CollisionWhitelist.isWhitelisted("jackson", null, ImmutableMap.of(
      "2.8.0", ImmutableList.of(JACKSON_2_8_0),
      "2.8.9", ImmutableList.of(JACKSON_2_8_9)
    )));

    // Correct Jackson weirdness - can also merge multiple major versions ...
    assertTrue(CollisionWhitelist.isWhitelisted("jackson", null, ImmutableMap.of(
      "1.3..0", ImmutableList.of(JACKSON_1_9_13),
      "2.8.0", ImmutableList.of(JACKSON_2_8_0_ANN),
      "2.8.9", ImmutableList.of(JACKSON_2_8_9)
    )));

    // 2.8.0 for API is allowed only, all other versions must match
    assertFalse(CollisionWhitelist.isWhitelisted("jackson", null, ImmutableMap.of(
      "2.8.0", ImmutableList.of(JACKSON_2_8_0_ANN),
      "2.8.9", ImmutableList.of(JACKSON_2_8_9),
      "2.3.5", ImmutableList.of(JACKSON_2_3_5)
    )));
  }

  @Test
  public void testCool() {
    Properties explicitWhitelist = new Properties();
    explicitWhitelist.setProperty("cool", "2.9.8,3.1.2");

    // Explicitly whitelisted versions
    assertTrue(CollisionWhitelist.isWhitelisted("cool", explicitWhitelist, ImmutableMap.of(
      "2.9.8", ImmutableList.of(COOL_2_9_8),
      "3.1.2", ImmutableList.of(COOL_3_1_2)
    )));


    // More versions then explicitly whitelisted
    assertFalse(CollisionWhitelist.isWhitelisted("cool", explicitWhitelist, ImmutableMap.of(
      "2.9.8", ImmutableList.of(COOL_2_9_8),
      "3.1.2", ImmutableList.of(COOL_3_1_2),
      "4.4.4", ImmutableList.of(COOL_4_4_4)
    )));
  }
}
