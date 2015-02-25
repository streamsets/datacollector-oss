/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.util;

import java.util.Iterator;
import java.util.Set;

public class StageUtil {

  public static String getCommaSeparatedNames(Set<String> names) {
    StringBuilder stringBuilder = new StringBuilder();
    if(names.size() > 0) {
      Iterator<String> iterator = names.iterator();
      stringBuilder.append(iterator.next());
      while(iterator.hasNext()) {
        stringBuilder.append(", ");
        stringBuilder.append(iterator.next());
      }
    }
    return stringBuilder.toString();
  }
}
