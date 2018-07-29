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
package com.streamsets.pipeline.lib.parser.net;

import com.google.common.primitives.Bytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class TestDelimitedLengthFieldBasedFrameDecoder {

  private static List<List<Byte>> getRandomByteSlices(byte[] bytes) {
    List<Byte> byteList = Bytes.asList(bytes);

    int numSlices = nextInt(2, 10);
    List<Integer> sliceIndexes = new ArrayList<Integer>();
    for (int i = 1; i <= numSlices; i++) {
      sliceIndexes.add(nextInt(0, bytes.length));
    }
    Collections.sort(sliceIndexes);

    List<List<Byte>> slices = new LinkedList<List<Byte>>();

    int byteInd = 0;
    for (int sliceIndex : sliceIndexes) {
      slices.add(byteList.subList(byteInd, sliceIndex));
      byteInd = sliceIndex;
    }
    slices.add(byteList.subList(byteInd, byteList.size()));

    return slices;
  }

  /**
   * Generates a new pseudo-random integer within the specific range.
   *
   * This is essentially the same method that is present in Apache commons-lang.  It is simply copied here to avoid
   * bringing in a new dependency
   *
   * @param startInclusive the lowest value that can be generated
   * @param endExclusive
   * @return a pseurandom number in [startInclusive, endExclusive)
   */
  private static int nextInt(final int startInclusive, final int endExclusive) {
    if (startInclusive == endExclusive) {
      return startInclusive;
    }

    return startInclusive + ThreadLocalRandom.current().nextInt(endExclusive - startInclusive);
  }

  @Test
  public void testDelimitedLengths() throws Exception {
    Charset charset = CharsetUtil.ISO_8859_1;
    EmbeddedChannel ch = getTestChannel(charset, 100, 0, false);

    String v1 = "a";
    String v2 = "abcdefghij";
    String v3 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrsxt"
        + "uvwxyz";

    writeStringAndAssert(ch, v1, charset, false, false);
    writeStringAndAssert(ch, v2, charset, false, false);
    writeStringAndAssert(ch, v3, charset, false, true);

    writeStringAndAssert(ch, v1, charset, true, false);
    writeStringAndAssert(ch, v2, charset, true, false);
    writeStringAndAssert(ch, v3, charset, true, true);

    writeStringAndAssert(ch, v1, charset, false, false);

    ch.close();
  }

  @Test
  public void testMaxFrameLengthOverflow() throws Exception {
    Charset charset = CharsetUtil.ISO_8859_1;
    // maxFrameLength plus adjustment would overflow an int
    final long numBytes = Integer.MAX_VALUE - 1;
    final int lengthAdjustment = 10;
    EmbeddedChannel ch = getTestChannel(charset, (int) numBytes, lengthAdjustment, true);

    //this is a bad frame, but will still test the overflow condition
    String longString = String.valueOf(numBytes) + " abcd";

    try {
      ch.writeInbound(Unpooled.copiedBuffer(longString, charset));
      Assert.fail("TooLongFrameException should have been thrown");
    } catch (TooLongFrameException ignored) {
      //ignored
    }
    Assert.assertNull(ch.readInbound());

    ch.close();
  }

  private EmbeddedChannel getTestChannel(
      Charset charset, int maxFrameLength, int lengthAdjustment, boolean failFast
  ) {
    return new EmbeddedChannel(new DelimitedLengthFieldBasedFrameDecoder(
        maxFrameLength,
        lengthAdjustment,
        failFast,
        Unpooled.copiedBuffer(" ", charset),
        charset,
        true
    ));
  }

  private void writeStringAndAssert(
      EmbeddedChannel channel,
      String value,
      Charset charset,
      boolean randomlyPartition,
      boolean expectFrameTooLarge
  ) {
    String frame = makeFrame(value, charset);

    try {
      if (randomlyPartition) {
        for (List<Byte> chunk : getRandomByteSlices(frame.getBytes())) {
          channel.writeInbound(Unpooled.copiedBuffer(Bytes.toArray(chunk)));
        }
      } else {
        channel.writeInbound(Unpooled.copiedBuffer(frame, charset));
      }
    } catch (TooLongFrameException e) {
      if (!expectFrameTooLarge) {
        Assert.fail("TooLongFrameException unexpectedly thrown");
      } else {
        Assert.assertNull(channel.readInbound());
      }
    }
    if (!expectFrameTooLarge) {
      ByteBuf in = (ByteBuf) channel.readInbound();
      Assert.assertEquals(value, in.toString(charset));
      in.release();
    }
  }

  private String makeFrame(String value, Charset charset) {
    int byteLength = value.getBytes(charset).length;

    return byteLength + " " + value;
  }
}
