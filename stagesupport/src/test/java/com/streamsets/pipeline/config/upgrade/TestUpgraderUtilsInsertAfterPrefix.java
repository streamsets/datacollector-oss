/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.pipeline.config.upgrade;

import com.streamsets.pipeline.api.Config;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

@RunWith(Parameterized.class)
public class TestUpgraderUtilsInsertAfterPrefix {
  private final String afterPrefix;
  private final Set<String> suffixes;
  private final String insertStr;
  private final List<Config> inputConfigs;
  private final int expectNumMoved;
  private final String[] expectedConfigMoves;

  public TestUpgraderUtilsInsertAfterPrefix(
      String afterPrefix,
      Set<String> suffixes,
      String insertStr,
      List<Config> inputConfigs,
      int expectNumMoved,
      String[] expectedConfigMoves
  ) {
    this.afterPrefix = afterPrefix;
    this.suffixes = suffixes;
    this.insertStr = insertStr;
    this.inputConfigs = inputConfigs;
    this.expectNumMoved = expectNumMoved;
    this.expectedConfigMoves = expectedConfigMoves;
  }

  @Parameterized.Parameters(name = "afterPrefix: {0}, suffixes: {1}, insertStr: {2}, expectNumMoved: {3}")
  public static Collection<Object[]> data() throws Exception {
    final List<Object[]> params = new LinkedList<>();
    params.add(new Object[] {
        "conf.",
        new HashSet<>(Arrays.asList("foo", "bar.field")),
        "newBean.",
        Arrays.asList(
            new Config("conf.foo", "1"),
            new Config("conf.bar.field", "2"),
            new Config("conf.baz", "3")
        ),
        2,
        new String[] {"conf.foo", "conf.newBean.foo", "conf.bar.field", "conf.newBean.bar.field"}
    });

    // case where nothing should be moved
    params.add(new Object[] {
        "conf.firstBean.",
        new HashSet<>(Arrays.asList("x", "y")),
        "newMiddle.",
        Arrays.asList(
            new Config("conf.secondBean.x", "1"),
            new Config("conf.secondBean.y", "2")
        ),
        0,
        new String[0]
    });
    return params;
  }

  @Test
  public void prependToAll() {
    final List<Config> configs = new LinkedList<>(inputConfigs);
    final UpgraderTestUtils.UpgradeMoveWatcher watcher = UpgraderTestUtils.snapshot(configs);
    final int numMoved = UpgraderUtils.insertAfterPrefix(configs, afterPrefix, suffixes, insertStr);
    Assert.assertEquals(expectNumMoved, numMoved);
    watcher.assertAllMoved(configs, expectedConfigMoves);
  }
}
