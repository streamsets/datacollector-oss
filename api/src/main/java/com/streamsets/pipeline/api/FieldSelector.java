/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@java.lang.annotation.Target(ElementType.FIELD)
/**
 * Marker annotation to be applied on a field which shall hold the names of
 * all the selected fields.
 *
 * The type of the field on which this annotation is applied should be "List<String>"
 */
public @interface FieldSelector {

  boolean singleValued() default false;
}
