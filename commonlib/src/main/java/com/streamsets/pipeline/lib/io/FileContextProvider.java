/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * The lifecycle of a file context provider is:
 *
 * repeat as needed:
 *
 *    setOffsets()
 *    loop while disFullLoop()
 *      next()
 *    optionally call startNewLoop() and do a new loop
 *    getOffsets()
 *
 *  close()
 *
 */
public interface FileContextProvider extends Closeable {

  void setOffsets(Map<String, String> offsets) throws IOException;

  FileContext next();

  boolean didFullLoop();

  void startNewLoop();

  Map<String, String> getOffsets() throws IOException;


}
