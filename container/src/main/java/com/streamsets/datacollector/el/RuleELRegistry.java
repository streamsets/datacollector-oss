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
package com.streamsets.datacollector.el;

import com.streamsets.datacollector.rules.AlertInfoEL;
import com.streamsets.datacollector.rules.DriftRuleEL;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.DataUnitsEL;
import com.streamsets.pipeline.lib.el.MathEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.lib.el.TimeEL;

/**
 * Registry which holds all EL functions which are useful for rules/conditions.
 */
public class RuleELRegistry {

  public static final String ALERT = "alert";
  public static final String GENERAL = "general";
  public static final String DRIFT = "drift";

  private static final String[] FAMILIES = {ALERT, GENERAL, DRIFT};

  private RuleELRegistry() {}

  public static String[] getFamilies() {
    return FAMILIES;
  }

  public static Class<?>[] getRuleELs(String family) {
    Utils.checkNotNull(family, "family");
    switch (family) {
      case GENERAL:
        return new Class[] {
            RecordEL.class,
            StringEL.class,
            MathEL.class,
            DataUnitsEL.class,
            RuntimeEL.class,
            JvmEL.class,
            TimeEL.class
        };
      case DRIFT:
        return new Class[] {
            RecordEL.class,
            StringEL.class,
            DataUnitsEL.class,
            RuntimeEL.class,
            JvmEL.class,
            DriftRuleEL.class
        };
      case ALERT:
        return new Class[] {
            RecordEL.class,
            StringEL.class,
            RuntimeEL.class,
            JvmEL.class,
            AlertInfoEL.class
        };
      default:
        throw new IllegalArgumentException(Utils.format("Invalid data rule family '{}'", family));
    }
  }

}
