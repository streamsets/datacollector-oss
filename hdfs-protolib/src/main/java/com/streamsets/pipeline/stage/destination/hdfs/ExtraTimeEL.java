/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;

public class ExtraTimeEL {

  @ElFunction(name = "every")
  public static String every(@ElParam("value") int value, @ElParam("timeUnitFunction") String unit) {
    return unit;
  }

}
