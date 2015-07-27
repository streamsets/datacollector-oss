/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.store;

import java.util.Date;

public class PipelineRevInfo {
  private final Date date;
  private final String user;
  private final String rev;
  private final String tag;
  private final String description;
  private final boolean valid;

  public PipelineRevInfo(PipelineInfo info) {
    date = info.getLastModified();
    user = info.getLastModifier();
    rev = info.getLastRev();
    tag = null;
    description = null;
    valid = info.isValid();
  }

  public PipelineRevInfo(Date date, String user, String rev, String tag, String description, boolean valid) {
    this.date = date;
    this.user = user;
    this.rev = rev;
    this.tag = tag;
    this.description = description;
    this.valid = valid;
  }

  public Date getDate() {
    return date;
  }

  public String getUser() {
    return user;
  }

  public String getRev() {
    return rev;
  }

  public String getTag() {
    return tag;
  }

  public String getDescription() {
    return description;
  }

  public boolean isValid() {
    return valid;
  }

}
