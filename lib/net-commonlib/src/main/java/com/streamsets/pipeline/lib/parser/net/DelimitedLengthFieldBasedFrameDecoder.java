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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.serialization.ObjectDecoder;

import java.nio.charset.Charset;
import java.util.List;

public class DelimitedLengthFieldBasedFrameDecoder extends ByteToMessageDecoder {

  private final int maxFrameLength;
  private final int lengthAdjustment;
  private final boolean failFast;
  private final Charset lengthFieldCharset;
  private final ByteBuf delimiter;
  private final boolean trimLengthString;

  private boolean discardingTooLongFrame;
  private long tooLongFrameLength;
  private long bytesToDiscard;
  private boolean consumingLength = true;
  private long frameLength = 0;

  public DelimitedLengthFieldBasedFrameDecoder(
      int maxFrameLength,
      int lengthAdjustment,
      boolean failFast,
      ByteBuf delimiter,
      Charset lengthFieldCharset,
      boolean trimLengthString
  ) {
    if (delimiter == null) {
      throw new NullPointerException("delimiter");
    }
    if (!delimiter.isReadable()) {
      throw new IllegalArgumentException("empty delimiter");
    }

    this.delimiter = delimiter.slice(delimiter.readerIndex(), delimiter.readableBytes());
    this.lengthFieldCharset = lengthFieldCharset;

    this.maxFrameLength = maxFrameLength;
    this.lengthAdjustment = lengthAdjustment;
    this.failFast = failFast;
    this.trimLengthString = trimLengthString;
  }

  @Override
  protected final void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    Object decoded = decode(ctx, in);
    if (decoded != null) {
      out.add(decoded);
    }
  }

  /**
   * Create a frame out of the {@link ByteBuf} and return it.
   *
   * @param ctx the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
   * @param in the {@link ByteBuf} from which to read data
   * @return frame           the {@link ByteBuf} which represent the frame or {@code null} if no frame could
   * be created.
   */
  protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
    if (discardingTooLongFrame) {
      long bytesToDiscard = this.bytesToDiscard;
      int localBytesToDiscard = (int) Math.min(bytesToDiscard, in.readableBytes());
      in.skipBytes(localBytesToDiscard);
      bytesToDiscard -= localBytesToDiscard;
      this.bytesToDiscard = bytesToDiscard;

      failIfNecessary(false);
      return null;
    }

    if (consumingLength) {
      int delimIndex = indexOf(in, delimiter);
      if (delimIndex < 0) {
        return null;
      }

      final String lengthStr = in.toString(in.readerIndex(), delimIndex, lengthFieldCharset);
      try {
        frameLength = Long.parseLong(trimLengthString ? lengthStr.trim() : lengthStr);
      } catch (NumberFormatException e) {
        throw new CorruptedFrameException(
            String.format(
                "Invalid length field decoded (in %s charset): %s",
                lengthFieldCharset.name(),
                lengthStr
            ),
            e
        );
      }

      if (frameLength < 0) {
        throw new CorruptedFrameException("negative pre-adjustment length field: " + frameLength);
      }

      frameLength += lengthAdjustment;

      //consume length field and delimiter bytes
      in.skipBytes(delimIndex + delimiter.capacity());

      //consume delimiter bytes
      consumingLength = false;
    }

    if (frameLength > maxFrameLength) {
      long discard = frameLength - in.readableBytes();
      tooLongFrameLength = frameLength;

      if (discard < 0) {
        // buffer contains more bytes then the frameLength so we can discard all now
        in.skipBytes((int) frameLength);
      } else {
        // Enter the discard mode and discard everything received so far.
        discardingTooLongFrame = true;
        consumingLength = true;
        bytesToDiscard = discard;
        in.skipBytes(in.readableBytes());
      }
      failIfNecessary(true);
      return null;
    }

    // never overflows because it's less than maxFrameLength
    int frameLengthInt = (int) frameLength;
    if (in.readableBytes() < frameLengthInt) {
      // need more bytes available to read actual frame
      return null;
    }

    // the frame is now entirely present, reset state vars
    consumingLength = true;
    frameLength = 0;

    // extract frame
    int readerIndex = in.readerIndex();
    int actualFrameLength = frameLengthInt;// - initialBytesToStrip;
    ByteBuf frame = extractFrame(ctx, in, readerIndex, actualFrameLength);
    in.readerIndex(readerIndex + actualFrameLength);
    return frame;
  }

  private void failIfNecessary(boolean firstDetectionOfTooLongFrame) {
    if (bytesToDiscard == 0) {
      // Reset to the initial state and tell the handlers that
      // the frame was too large.
      long prevTooLongFrameLength = tooLongFrameLength;
      tooLongFrameLength = 0;
      discardingTooLongFrame = false;
      consumingLength = true;
      frameLength = 0;
      if (!failFast || firstDetectionOfTooLongFrame) {
        fail(prevTooLongFrameLength);
      }
    } else {
      // Keep discarding and notify handlers if necessary.
      if (failFast && firstDetectionOfTooLongFrame) {
        fail(tooLongFrameLength);
      }
    }
  }

  /**
   * Extract the sub-region of the specified buffer.
   * <p>
   * If you are sure that the frame and its content are not accessed after
   * the current {@link #decode(ChannelHandlerContext, ByteBuf)}
   * call returns, you can even avoid memory copy by returning the sliced
   * sub-region (i.e. <tt>return buffer.slice(index, length)</tt>).
   * It's often useful when you convert the extracted frame into an object.
   * Refer to the source code of {@link ObjectDecoder} to see how this method
   * is overridden to avoid memory copy.
   */
  protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
    return buffer.slice(index, length).retain();
  }

  private void fail(long frameLength) {
    if (frameLength > 0) {
      throw new TooLongFrameException("Adjusted frame length exceeds " + maxFrameLength + ": " + frameLength + " - " +
          "discarded");
    } else {
      throw new TooLongFrameException("Adjusted frame length exceeds " + maxFrameLength + " - discarding");
    }
  }

  /**
   * Returns the number of bytes between the readerIndex of the haystack and
   * the first needle found in the haystack.  -1 is returned if no needle is
   * found in the haystack.
   */
  private static int indexOf(ByteBuf haystack, ByteBuf needle) {
    for (int i = haystack.readerIndex(); i < haystack.writerIndex(); i ++) {
      int haystackIndex = i;
      int needleIndex;
      for (needleIndex = 0; needleIndex < needle.capacity(); needleIndex ++) {
        if (haystack.getByte(haystackIndex) != needle.getByte(needleIndex)) {
          break;
        } else {
          haystackIndex ++;
          if (haystackIndex == haystack.writerIndex() &&
              needleIndex != needle.capacity() - 1) {
            return -1;
          }
        }
      }

      if (needleIndex == needle.capacity()) {
        // Found the needle from the haystack!
        return i - haystack.readerIndex();
      }
    }
    return -1;
  }
}
