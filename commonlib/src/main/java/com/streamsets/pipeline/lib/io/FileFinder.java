/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

}
