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
package com.streamsets.datacollector.runner.production;

import com.streamsets.pipeline.api.Source;

import java.util.Collections;

/**
 * Upgrader for SourceOffset file.
 */
public class SourceOffsetUpgrader {

  public static void upgrade(SourceOffset sourceOffset) {
    // Shortcut if we don't need an upgrade
    if(sourceOffset.getVersion() == SourceOffset.CURRENT_VERSION) {
      return;
    }

    switch(sourceOffset.getVersion()) {
      case 0:
        // fall through
      case 1:
        upgradeV0toV2(sourceOffset);
        // fall through
      case 2:
        // Current version
        sourceOffset.setVersion(2);
        break;
      default:
        throw new IllegalArgumentException("Unknown SourceOffset version: " + sourceOffset.getVersion());
    }

  }

  private static void upgradeV0toV2(SourceOffset sourceOffset) {
    sourceOffset.setOffsets(Collections.singletonMap(Source.POLL_SOURCE_OFFSET_KEY, sourceOffset.getOffset()));
    sourceOffset.setOffset(null);
  }
}
