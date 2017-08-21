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
package com.streamsets.datacollector.bundles;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation describing Support Bundle Content generator and it's metadata.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface BundleContentGeneratorDef {

  /**
   * User visible name of the bundle.
   */
  String name();

  /**
   * Internal id to be used when requesting what generators should be used for given bundle. Default is empty value
   * that will get resolved to the class name (without package name). The id of generators must be unique across the
   * data collector instance.
   */
  String id() default "";

  /**
   * User visible description (few sentences) about what this generator does - what kind of data it collects.
   */
  String description();

  /**
   * Version of the content generator.
   */
  int version();

  /**
   * Whether this content generator should be used by default when user is not specifying what generators should be used.
   */
  boolean enabledByDefault() default false;

  /**
   * Suggested order in which the generators should be used.
   *
   * If two generators share the same number, they will run in non-deterministic order. Otherwise generator with smaller
   * number will always run first.
   */
  int order() default 0;
}
