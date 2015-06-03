/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

/**
 * Finds new files that show up in a search location. Files are found once then remembered. If a file is forgotten,
 * it will be found again.
 * <p/>
 * Implementations may do the finding synchronously or asynchronously from the find() method. The only requirement
 * is that first invocation to find() returns all the currently available files (this can be thought as the first
 * invocation being always synchronous).
 */
public interface FileFinder {

  Set<Path> find() throws IOException;

  boolean forget(Path path);

  void close();
}
