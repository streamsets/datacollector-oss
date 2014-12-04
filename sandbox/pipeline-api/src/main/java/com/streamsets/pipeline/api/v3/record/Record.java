/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.api.v3.record;

import com.streamsets.pipeline.api.v3.record.Field;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;

public interface Record {

  public interface Header {

    public String getPipelineFingerprint();

    public String getSource();

    public List<String> getPipelinePath();

    public List<String> getTags();

    public byte[] getRaw();

    public String getRawMime();

    public Iterator<String> getAttributeNames();

    public String getAttribute(String name);

    public void setAttribute(String name, String value);

    public String toString();

  }

  public Header getHeader();

  public Iterator<String> getFieldNames();

  public Field getField(String name);

  public void setField(String name, Field field);

  public void deleteField(String name);

  public String toString(Locale locale);

}
