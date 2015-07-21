/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.websockets;

public interface ListenerManager<L> {

  public void register(L listener);

  public void unregister(L listener);

}
