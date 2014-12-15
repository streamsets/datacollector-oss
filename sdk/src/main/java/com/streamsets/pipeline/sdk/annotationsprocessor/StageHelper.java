/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk.annotationsprocessor;

public class StageHelper {

  public static String getStageNameFromClassName(String qualifiedClassName) {
    return qualifiedClassName.replace(".", "_");
  }

}
