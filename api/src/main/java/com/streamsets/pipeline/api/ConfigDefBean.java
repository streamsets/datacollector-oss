/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */

package com.streamsets.pipeline.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ConfigDefBean {

  // groups must be a valid group from the StageDef
  // or an ordinal reference to the parent's groups (StageDef or other ConfigDefBean)
  // a reference to the parent's group is done via '#N' where N is the ordinal group name of the parent
  String[] groups() default {};

}
