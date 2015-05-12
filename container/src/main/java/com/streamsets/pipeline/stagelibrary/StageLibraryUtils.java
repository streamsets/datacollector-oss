/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stagelibrary;

import com.streamsets.pipeline.api.impl.LocalizableMessage;

import java.lang.reflect.Method;

public class StageLibraryUtils {
  private static final String LIBRARY_RB = "data-collector-library";

  public static String getLibraryName(ClassLoader cl) {
    String name;
    try {
      Method method = cl.getClass().getMethod("getName");
      name = (String) method.invoke(cl);
    } catch (NoSuchMethodException ex ) {
      name = "default";
    } catch (Exception ex ) {
      throw new RuntimeException(ex);
    }
    return name;
  }

  public static String getLibraryType(ClassLoader cl) {
    String name = null;
    try {
      Method method = cl.getClass().getMethod("getType");
      method.setAccessible(true);
      name = (String) method.invoke(cl);
    } catch (NoSuchMethodException ex ) {
      // ignore
    } catch (Exception ex ) {
      throw new RuntimeException(ex);
    }
    return name;
  }

  public static String getLibraryLabel(ClassLoader cl) {
    String label = getLibraryName(cl);
    LocalizableMessage lm = new LocalizableMessage(cl, LIBRARY_RB, "library.name", label, new Object[0]);
    return lm.getLocalized();
  }
}
