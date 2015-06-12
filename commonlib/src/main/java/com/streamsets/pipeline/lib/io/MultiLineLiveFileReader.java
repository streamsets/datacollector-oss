/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.io;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.impl.Utils;
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
  private final LiveFileReader reader;
  private final Pattern pattern;
  private LiveFileChunk lookAheadChunk;
  private boolean lookAheadTruncated;
  private final List<FileLine> incompleteMultiLine;
  private long incompleteMultiLinesLen;

  /**
   * Creates a multi line reader.
   *
   * @param reader The single line reader to use.
   * @param mainLinePattern the regex pattern that determines if a line is main line.
   */
  public MultiLineLiveFileReader(LiveFileReader reader, Pattern mainLinePattern) {
    this.reader = reader;
    this.pattern = mainLinePattern;
    incompleteMultiLine = new ArrayList<>();
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
    return reader.getOffset() - incompleteMultiLinesLen;
  }

  @Override
  public boolean hasNext() throws IOException {
    // if the underlying reader is EOF we still have to flush the current incomplete multi line as a complete multi line
    // so we return true if we have incomplete multi lines
    return reader.hasNext() || !incompleteMultiLine.isEmpty();
  }

  @Override
  public LiveFileChunk next(long waitMillis) throws IOException {
    LiveFileChunk chunk = null;
    if (!reader.hasNext()) {
      Utils.checkState(lookAheadChunk != null, Utils.formatL("LiveFileReader for '{}' has reached EOL",
                                                            reader.getLiveFile()));
      // if the underlying reader is EOF we still have to flush the current incomplete multi line as a complete multi
      // line if we have incomplete multi lines (lookAheadChunk != null)
      chunk = flushLookAhead();
    } else {
      // get new chunk from underlying reader
      LiveFileChunk newChunk = reader.next(waitMillis);
      if (newChunk != null) {
        chunk = compactChunk(newChunk);
      }
    }
    return chunk;
  }

  LiveFileChunk flushLookAhead() {
    LiveFileChunk chunk = new LiveFileChunk(lookAheadChunk.getTag(), lookAheadChunk.getFile(),
                                            lookAheadChunk.getCharset(),
                                            ImmutableList.of(compactFileLines(incompleteMultiLine)),
                                            lookAheadChunk.isTruncated());
    incompleteMultiLine.clear();
    incompleteMultiLinesLen = 0;
    lookAheadChunk = null;
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
  LiveFileChunk compactChunk(LiveFileChunk chunk) {
    List<FileLine> completeLines = new ArrayList<>();
    List<FileLine> chunkLines = chunk.getLines();
    if (lookAheadChunk == null) {
      lookAheadChunk = chunk;
      lookAheadTruncated = chunk.isTruncated();
    }
    lookAheadTruncated |= chunk.isTruncated();

    int pos = 0;
    int idx = findNextMainLine(chunk, pos);

    // while we have main lines we keep adding/compacting into the new chunk
    while (idx > -1) {

      //any multi lines up to the next main line belong to the previous main line
      for (int i = pos; i < idx; i++) {
        incompleteMultiLine.add(chunkLines.get(i));
        incompleteMultiLinesLen += chunkLines.get(i).getLength();
      }

      // if we have incomplete lines, at this point they are a complete multiline, compact and add to new chunk lines
      if (!incompleteMultiLine.isEmpty()) {
        completeLines.add(compactFileLines(incompleteMultiLine));
      }

      // clear the incomplete multi lines as we just used them to create a full line
      incompleteMultiLine.clear();
      incompleteMultiLinesLen = 0;

      // add the current main line as incomplete as we still don't if it is a complete line
      incompleteMultiLine.add(chunkLines.get(idx));
      incompleteMultiLinesLen += chunkLines.get(idx).getLength();

      // find the next main line
      pos = idx + 1;
      idx = findNextMainLine(chunk, pos);
    }

    // lets process the left over multi lines in the chunk after the last main line.
    // if any they will kept to completed with lines from the next chunk.
    for (int i = pos; i < chunkLines.size(); i++) {
      incompleteMultiLine.add(chunkLines.get(i));
      incompleteMultiLinesLen += chunkLines.get(i).getLength();
    }

    // create a new chunk with all complete multi lines
    chunk = new LiveFileChunk(chunk.getTag(), chunk.getFile(), chunk.getCharset(), completeLines, lookAheadTruncated);

    return chunk;
  }

  // compacts a list of lines into a single line. the assumptions is that only the first line of the list is a main line
  FileLine compactFileLines(List<FileLine> lines) {
    FileLine fileLine;
    boolean sameBuffer = true;
    FileLine firstLine = lines.get(0);
    int len = firstLine.getLength();

    // compute new line length and finds out if all the lines are in the same buffer
    for (int i = 1; i < lines.size(); i++) {
      sameBuffer &= firstLine.getBuffer() == lines.get(i).getBuffer();
      len += lines.get(i).getLength();
    }

    if (sameBuffer) {
      // the lines are in the same buffer they buffer is consecutive and can create a line using the original
      // buffer without copying any data
      fileLine = new FileLine(firstLine.getCharset(), firstLine.getFileOffset(), firstLine.getBuffer(),
                              firstLine.getOffset(), len);
    } else {
      // the lines are not i nthe same buffer, we need to copy their contents to a new buffer.
      byte[] buffer = new byte[len];
      int pos = 0;
      for (FileLine line : lines) {
        System.arraycopy(line.getBuffer(), line.getOffset(), buffer, pos, line.getLength());
        pos += line.getLength();
      }
      fileLine = new FileLine(firstLine.getCharset(), firstLine.getFileOffset(), buffer, 0, pos);
    }
    return fileLine;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

}
