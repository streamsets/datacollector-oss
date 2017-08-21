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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.lib.parser.shaded.com.google.code.regexp.Pattern;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * A <code>MultiLineLiveFileReader</code> is Reader that allows to read a file in a 'tail -f' mode while keeping track
 * of the current offset and detecting if the file has been renamed.
 * <p/>
 * It tails multi line files (i.e. Log4j logs with stack traces, MySQL logs)
 * <p/>
 * It should be used wrapping a {@link SingleLineLiveFileReader}.
 * <p/>
 * All lines that do not match the main line pattern are collapsed on the previous main line. The implementation works
 * doing a look ahead.
 * <p/>
 * The assumption is that multi lines do not continue in other files. So when we read the EOF we flush the last
 * accumulated multi line as a complete multi line.
 */
public class MultiLineLiveFileReader implements LiveFileReader {
  private final String tag;
  private final LiveFileReader reader;
  private final Pattern pattern;
  private boolean incompleteMultiLineTruncated;
  private final StringBuilder incompleteMultiLine;
  private long incompleteMultiLineOffset;

  /**
   * Creates a multi line reader.
   *
   * @param reader The single line reader to use.
   * @param mainLinePattern the regex pattern that determines if a line is main line.
   */
  public MultiLineLiveFileReader(String tag, LiveFileReader reader, Pattern mainLinePattern) {
    this.tag = tag;
    this.reader = reader;
    this.pattern = mainLinePattern;
    incompleteMultiLine = new StringBuilder(2048);
  }

  @Override
  public LiveFile getLiveFile() {
    return reader.getLiveFile();
  }

  @Override
  public Charset getCharset() {
    return reader.getCharset();
  }

  @Override
  public long getOffset() {
    // we have to correct the reader offset with the length of the incomplete multi lines as that is logical position
    // for user of the multi line reader
    return reader.getOffset() - incompleteMultiLine.length();
  }

  @Override
  public boolean hasNext() throws IOException {
    // if the underlying reader is EOF we still have to flush the current incomplete multi line as a complete multi line
    // so we return true if we have incomplete multi lines
    return reader.hasNext() || incompleteMultiLine.length() != 0;
  }

  @Override
  public LiveFileChunk next(long waitMillis) throws IOException {
    LiveFileChunk chunk = null;
    if (!reader.hasNext()) {
      if (incompleteMultiLine.length() > 0) {
        // the underlying reader is EOF, we still have to return the current incomplete multiline.
        // now we know is as a complete multiline because we reached EOF
        chunk = new LiveFileChunk(tag, reader.getLiveFile(), reader.getCharset(),
                                  ImmutableList.of(new FileLine(incompleteMultiLineOffset, incompleteMultiLine.toString())),
                                  incompleteMultiLineTruncated);
        incompleteMultiLine.setLength(0);
      }
    } else {
      // get new chunk from underlying reader
      LiveFileChunk newChunk = reader.next(waitMillis);
      if (newChunk != null) {
        chunk = resolveChunk(newChunk);
      }
    }
    return chunk;
  }

  // finds the first main line in the chunk from the specified index position onwards
  int findNextMainLine(LiveFileChunk chunk, int startIdx) {
    List<FileLine> lines = chunk.getLines();
    int found = -1;
    for (int i = startIdx; found == -1 && i < lines.size(); i++) {
      if (pattern.matcher(lines.get(i).getText().trim()).matches()) {
        found = i;
      }
    }
    return found;
  }

  // compacts all multi lines of chunk into single lines.
  // it there is an incomplete multiline from a previous chunk it starts from it.
  LiveFileChunk resolveChunk(LiveFileChunk chunk) {
    List<FileLine> completeLines = new ArrayList<>();
    List<FileLine> chunkLines = chunk.getLines();
    if (incompleteMultiLine.length() == 0) {
      incompleteMultiLineOffset = chunk.getOffset();
      incompleteMultiLineTruncated = chunk.isTruncated();
    }
    incompleteMultiLineTruncated |= chunk.isTruncated();

    int pos = 0;
    int idx = findNextMainLine(chunk, pos);

    // while we have main lines we keep adding/compacting into the new chunk
    while (idx > -1) {

      //any multi lines up to the next main line belong to the previous main line
      for (int i = pos; i < idx; i++) {
        incompleteMultiLine.append(chunkLines.get(i).getText());
      }

      // if we have incomplete lines, at this point they are a complete multiline, compact and add to new chunk lines
      if (incompleteMultiLine.length() != 0) {
        completeLines.add(new FileLine(incompleteMultiLineOffset, incompleteMultiLine.toString()));
        incompleteMultiLineOffset += incompleteMultiLine.length();
        // clear the incomplete multi lines as we just used them to create a full line
        incompleteMultiLine.setLength(0);
        incompleteMultiLineTruncated = false;
      }

      // add the current main line as incomplete as we still don't if it is a complete line
      incompleteMultiLine.append(chunkLines.get(idx).getText());

      // find the next main line
      pos = idx + 1;
      idx = findNextMainLine(chunk, pos);
    }

    // lets process the left over multi lines in the chunk after the last main line.
    // if any they will kept to completed with lines from the next chunk.
    for (int i = pos; i < chunkLines.size(); i++) {
      incompleteMultiLine.append(chunkLines.get(i).getText());
    }

    if (completeLines.isEmpty()) {
      // didn't get a complete multi line yet, we keep storing lines but return a null chunk
      chunk = null;
    } else {
      // create a new chunk with all complete multi lines
      chunk = new LiveFileChunk(chunk.getTag(), chunk.getFile(), chunk.getCharset(), completeLines,
                                incompleteMultiLineTruncated);
    }
    return chunk;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

}
