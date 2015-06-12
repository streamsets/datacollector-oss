/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Reader interface to read live files. There is an implementation tha reads single lines, {@link }LineLiveFileReader},
 * and an implementation that reads multi lines, {@link MultiLineLiveFileReader}, which is useful for reading log files
 * with multi-line logs.
 */
public interface LiveFileReader extends Closeable {

  /**
   * Returns the {@link LiveFile} of the reader.
   *
   * @return the {@link LiveFile} of the reader.
   */
  LiveFile getLiveFile();

  /**
   * Returns the charset of the reader.
   *
   * @return  the charset of the reader.
   */
  Charset getCharset();

  /**
   * Returns the reader offset.
   * <p/>
   * NOTE: The reader offset will be a negative number of the reader is in truncate mode (the last line of the
   * last chunk exceeded the maximum length).
   * @return the reader offset.
   */
  long getOffset();

  /**
   * Indicates if the reader has more data or the EOF has been reached. Note that if the {@link LiveFile} is the original
   * one and we are at the EOF we are in 'tail -f' mode, only when the file has been renamed we reached EOF.
   *
   * @return <code>true</code> if the reader has more data, <code>false</code> otherwise.
   * @throws IOException thrown if there was an error while determining if there is more data or not.
   */
  boolean hasNext() throws IOException;

  /**
   * Returns the next chunk of data from the reader if available, or <code>null</code> if there is no data available
   * yet.
   * <p/>
   * This method can be called only if {@link #hasNext()} returned <code>true</code>.
   *
   * @param waitMillis milliseconds to block while waiting for more data, use zero for no wait.
   * @return a {@link LiveFileChunk} with a chunk of data, or <code>null</code> if no data is yet available.
   * @throws IOException thrown if there was a problem while reading the chunk of data
   */
  LiveFileChunk next(long waitMillis) throws IOException;

  /**
   * Closes the reader.
   *
   * @throws IOException thrown if the reader could not be closed properly.
   */
  @Override
  void close() throws IOException;
}
