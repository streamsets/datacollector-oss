/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.el;

/**
 * Registry which holds all EL functions which are useful for rules/conditions.
 */
public class RuleELRegistry {
  public static Class<?>[] getRuleELs() {
    return new Class[] {
      RecordEL.class,
      StringEL.class,
      ConditionUtilEL.class
    };
  }
}
