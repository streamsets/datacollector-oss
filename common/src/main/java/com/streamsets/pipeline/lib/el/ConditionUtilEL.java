/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.el;

import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.UUID;

/**
 * EL functions useful when evaluating conditions. These maybe useful
 * when transforming data as well.
 */
public class ConditionUtilEL {
  @ElFunction(prefix = "", name = "humanReadableToBytes", description = "Converts human readable byte " +
    "specifications to the number of bytes. E.g. 1KB to 1,000")
  public static long humanReadableToBytes(@ElParam("value") String value) {
    return Utils.humanReadableToBytes(value);
  }
}
