/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public interface Record {

  public static final String MODULE = "_.module";

  public static final String SOURCE = "_.source";

  public static final String PROCESSING_PATH = "_.path";

  public interface Header {

    public byte[] getRaw();

    public String getRawMime();

    public Iterator<String> getAttributeNames();

    public String getAttribute(String name);

    public void setAttribute(String name, String value);

    public void removeAttribute(String name);

  }

  public Record getPrevious();

  public Header getHeader();

  public Iterator<String> getFieldNames();

  public Field getField(String name);

  public void setField(String name, Field field);

  public void deleteField(String name);

  public String toString(Locale locale);

}
