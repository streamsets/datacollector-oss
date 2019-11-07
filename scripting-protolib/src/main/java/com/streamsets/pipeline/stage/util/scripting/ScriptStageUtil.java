/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.util.scripting;

import com.streamsets.pipeline.api.Stage;
import org.slf4j.Logger;

import javax.script.ScriptEngine;
import java.io.Closeable;

public final class ScriptStageUtil {
  private ScriptStageUtil() {
    // empty, private constructor for static util class
  }

  /**
   * Utility method for closing an {@link ScriptEngine}.  There is no generic concept of cleanup defined in that
   * interface, but some implementation (i.e. the engines for specific languages) also implement
   * {@link AutoCloseable} or {@link Closeable} to perform cleanup, so we will attempt each of those in turn.
   *
   * @param engine the instance of {@link ScriptEngine} to close
   * @param logger the {@link Logger} to log messages to
   */
  public static void closeEngine(ScriptEngine engine, Stage.Info stageInfo, Logger logger) {
    if (logger.isTraceEnabled()) {
      logger.trace(
          "About to close {} engine for {} stage instance {}",
          engine.getFactory().getEngineName(),
          stageInfo.getName(),
          stageInfo.getInstanceName()
      );
    }
    try {
      if (engine instanceof AutoCloseable) {
        // AutoCloseable pertains to try-with-resources, but since our stage lifecycle doesn't
        // comport with try-with-resources, we have to call this manually
        ((AutoCloseable) engine).close();
      } else if (engine instanceof Closeable) {
        ((Closeable) engine).close();
      }
    } catch (Exception e) {
      logger.error(Errors.SCRIPTING_11.getMessage(), e.getClass().getSimpleName(), e.getMessage(), e);
    }
  }
}
