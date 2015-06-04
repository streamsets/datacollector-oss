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
public abstract class FileFinder {

  public abstract Set<Path> find() throws IOException;

  public abstract boolean forget(Path path);

  public abstract void close();

  public static boolean hasGlobWildcard(String name) {
    boolean escaped = false;
    for (char c: name.toCharArray()) {
      if (c == '\\') {
        escaped = true;
      } else {
        if (!escaped) {
          switch (c) {
            case '*':
            case '?':
            case '{':
            case '[':
              return true;
          }
        } else {
          escaped = false;
        }
      }
    }
    return false;
  }

}
