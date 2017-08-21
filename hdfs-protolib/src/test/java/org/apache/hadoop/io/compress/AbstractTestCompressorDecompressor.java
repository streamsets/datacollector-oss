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
package org.apache.hadoop.io.compress;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractTestCompressorDecompressor {

  protected abstract int getDefaultBufferSize();

  protected abstract Compressor createCompressor();

  protected abstract Compressor createCompressor(int bufferSize);

  protected abstract Decompressor createDecompressor();

  protected abstract Decompressor createDecompressor(int bufferSize);


  @Test
  public void testCompressorSetInputNullPointerException() {
    try {
      Compressor compressor = createCompressor();
      compressor.setInput(null, 0, 10);
      fail("testCompressorSetInputNullPointerException error !!!");
    } catch (NullPointerException ex) {
      // excepted
    } catch (Exception ex) {
      fail("testCompressorSetInputNullPointerException ex error !!!");
    }
  }

  @Test
  public void testDecompressorSetInputNullPointerException() {
    try {
      Decompressor decompressor = createDecompressor();
      decompressor.setInput(null, 0, 10);
      fail("testDecompressorSetInputNullPointerException error !!!");
    } catch (NullPointerException ex) {
      // expected
    } catch (Exception e) {
      fail("testDecompressorSetInputNullPointerException ex error !!!");
    }
  }

  @Test
  public void testCompressorSetInputAIOBException() {
    try {
      Compressor compressor = createCompressor();
      compressor.setInput(new byte[] {}, -5, 10);
      fail("testCompressorSetInputAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception ex) {
      fail("testCompressorSetInputAIOBException ex error !!!");
    }
  }

  @Test
  public void testDecompressorSetInputAIOUBException() {
    try {
      Decompressor decompressor = createDecompressor();
      decompressor.setInput(new byte[] {}, -5, 10);
      fail("testDecompressorSetInputAIOUBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception e) {
      fail("testDecompressorSetInputAIOUBException ex error !!!");
    }
  }

  @Test
  public void testCompressorCompressNullPointerException() {
    try {
      Compressor compressor = createCompressor();
      byte[] bytes = BytesGenerator.get(1024 * 6);
      compressor.setInput(bytes, 0, bytes.length);
      compressor.compress(null, 0, 0);
      fail("testCompressorCompressNullPointerException error !!!");
    } catch (NullPointerException ex) {
      // expected
    } catch (Exception e) {
      fail("testCompressorCompressNullPointerException ex error !!!");
    }
  }

  @Test
  public void testDecompressorCompressNullPointerException() {
    try {
      Decompressor decompressor = createDecompressor();
      byte[] bytes = BytesGenerator.get(1024 * 6);
      decompressor.setInput(bytes, 0, bytes.length);
      decompressor.decompress(null, 0, 0);
      fail("testDecompressorCompressNullPointerException error !!!");
    } catch (NullPointerException ex) {
      // expected
    } catch (Exception e) {
      fail("testDecompressorCompressNullPointerException ex error !!!");
    }
  }

  @Test
  public void testCompressorCompressAIOBException() {
    try {
      Compressor compressor = createCompressor();
      byte[] bytes = BytesGenerator.get(1024 * 6);
      compressor.setInput(bytes, 0, bytes.length);
      compressor.compress(new byte[] {}, 0, -1);
      fail("testCompressorCompressAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception e) {
      fail("testCompressorCompressAIOBException ex error !!!");
    }
  }

  @Test
  public void testDecompressorCompressAIOBException() {
    try {
      Decompressor decompressor = createDecompressor();
      byte[] bytes = BytesGenerator.get(1024 * 6);
      decompressor.setInput(bytes, 0, bytes.length);
      decompressor.decompress(new byte[] {}, 0, -1);
      fail("testDecompressorCompressAIOBException error !!!");
    } catch (ArrayIndexOutOfBoundsException ex) {
      // expected
    } catch (Exception e) {
      fail("testDecompressorCompressAIOBException ex error !!!");
    }
  }

  @Test
  public void testCompressDecompress() {
    int BYTE_SIZE = 1024 * 54;
    byte[] bytes = BytesGenerator.get(BYTE_SIZE);
    Compressor compressor = createCompressor();
    try {
      compressor.setInput(bytes, 0, bytes.length);
      assertTrue("CompressDecompress getBytesRead error !!!",
                 compressor.getBytesRead() > 0);
      assertTrue(
          "CompressDecompress getBytesWritten before compress error !!!",
          compressor.getBytesWritten() == 0);

      byte[] compressed = new byte[BYTE_SIZE];
      int cSize = compressor.compress(compressed, 0, compressed.length);
      assertTrue(
          "CompressDecompress getBytesWritten after compress error !!!",
          compressor.getBytesWritten() > 0);

      Decompressor decompressor = createDecompressor(BYTE_SIZE);
      // set as input for decompressor only compressed data indicated with cSize
      decompressor.setInput(compressed, 0, cSize);
      byte[] decompressed = new byte[BYTE_SIZE];
      decompressor.decompress(decompressed, 0, decompressed.length);

      assertTrue("testCompressDecompress finished error !!!",
                 decompressor.finished());
      Assert.assertArrayEquals(bytes, decompressed);
      compressor.reset();
      decompressor.reset();
      assertTrue("decompressor getRemaining error !!!",
                 decompressor.getRemaining() == 0);
    } catch (Exception e) {
      fail("testCompressDecompress ex error!!!");
    }
  }

  @Test
  public void testCompressorDecompressorEmptyStreamLogic() {
    ByteArrayInputStream bytesIn = null;
    ByteArrayOutputStream bytesOut = null;
    byte[] buf = null;
    BlockDecompressorStream blockDecompressorStream = null;
    try {
      // compress empty stream
      bytesOut = new ByteArrayOutputStream();
      BlockCompressorStream blockCompressorStream = new BlockCompressorStream(
          bytesOut, createCompressor(), 1024, 0);
      // close without write
      blockCompressorStream.close();

      // check compressed output
      buf = bytesOut.toByteArray();
      assertEquals("empty stream compressed output size != 4", 4, buf.length);

      // use compressed output as input for decompression
      bytesIn = new ByteArrayInputStream(buf);

      // create decompression stream
      blockDecompressorStream = new BlockDecompressorStream(bytesIn,
                                                            createDecompressor(), 1024);

      // no byte is available because stream was closed
      assertEquals("return value is not -1", -1, blockDecompressorStream.read());
    } catch (Exception e) {
      fail("testCompressorDecompressorEmptyStreamLogic ex error !!!"
           + e.getMessage());
    } finally {
      if (blockDecompressorStream != null)
        try {
          bytesIn.close();
          bytesOut.close();
          blockDecompressorStream.close();
        } catch (IOException e) {
        }
    }
  }

  @Test
  public void testBlockCompression() {
    int BYTE_SIZE = 1024 * 50;
    int BLOCK_SIZE = 512;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] block = new byte[BLOCK_SIZE];
    byte[] bytes = BytesGenerator.get(BYTE_SIZE);
    try {
      // Use default of 512 as bufferSize and compressionOverhead of
      // (1% of bufferSize + 12 bytes) = 18 bytes (zlib algorithm).
      Compressor compressor = createCompressor();
      int off = 0;
      int len = BYTE_SIZE;
      int maxSize = BLOCK_SIZE - 18;
      if (BYTE_SIZE > maxSize) {
        do {
          int bufLen = Math.min(len, maxSize);
          compressor.setInput(bytes, off, bufLen);
          compressor.finish();
          while (!compressor.finished()) {
            compressor.compress(block, 0, block.length);
            out.write(block);
          }
          compressor.reset();
          off += bufLen;
          len -= bufLen;
        } while (len > 0);
      }
      assertTrue("testBlockCompression error !!!",
                 out.toByteArray().length > 0);
    } catch (Exception ex) {
      fail("testBlockCompression ex error !!!");
    }
  }

  private void compressDecompressLoop(int rawDataSize) throws IOException {
    byte[] rawData = BytesGenerator.get(rawDataSize);
    byte[] compressedResult = new byte[rawDataSize + 20];

    Compressor compressor = createCompressor(rawDataSize + 20);
    compressor.setInput(rawData, 0, rawDataSize);
    compressor.finish();

    int compressedLen = 0;
    while (!compressor.finished()) {
      int n = compressor.compress(compressedResult, compressedLen, compressedResult.length - compressedLen);
      compressedLen += n;
    }

    Decompressor decompressor = createDecompressor(rawDataSize + 20);
    decompressor.setInput(compressedResult, 0, compressedLen);

    byte[] gotBack = new byte[rawDataSize];
    int gotBackOffset = 0;

    byte[] buff = new byte[getDefaultBufferSize()];

    while(!decompressor.finished()) {
      int n = decompressor.decompress(buff, 0, buff.length);
      System.arraycopy(buff, 0, gotBack, gotBackOffset, n);
      gotBackOffset += n;
    }
    Assert.assertEquals(rawDataSize, gotBackOffset);
    Assert.assertArrayEquals(rawData, gotBack);
  }

  @Test
  public void testCompressionDecompressionDiffBuffersDiffDataLength() {
    int[] size = { 4 * 1024, 64 * 1024, 128 * 1024, 1024 * 1024 };
    try {
      for (int i = 0; i < size.length; i++) {
        compressDecompressLoop(size[i]);
      }
    } catch (IOException ex) {
      fail("testDirectBlockCompression ex !!!" + ex);
    }
  }

  @Test
  public void testCompressorDecompressorLogicWithCompressionStreams() {
    int BYTE_SIZE = 1024 * 100;
    byte[] bytes = BytesGenerator.get(BYTE_SIZE);
    int bufferSize = 262144;
    int compressionOverhead = (bufferSize / 6) + 32;
    DataOutputStream deflateOut = null;
    DataInputStream inflateIn = null;
    try {
      DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
      CompressionOutputStream deflateFilter = new BlockCompressorStream(
          compressedDataBuffer, createCompressor(bufferSize), bufferSize,
          compressionOverhead);
      deflateOut = new DataOutputStream(new BufferedOutputStream(deflateFilter));

      deflateOut.write(bytes, 0, bytes.length);
      deflateOut.flush();
      deflateFilter.finish();

      DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
      deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0,
                                   compressedDataBuffer.getLength());

      CompressionInputStream inflateFilter = new BlockDecompressorStream(
          deCompressedDataBuffer, createDecompressor(bufferSize),
          bufferSize);

      inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));

      byte[] result = new byte[BYTE_SIZE];
      inflateIn.read(result);

      Assert.assertArrayEquals(
          "original array not equals compress/decompressed array", result,
          bytes);
    } catch (IOException e) {
      fail("testCompressorDecompressorLogicWithCompressionStreams ex error !!!");
    } finally {
      try {
        if (deflateOut != null)
          deflateOut.close();
        if (inflateIn != null)
          inflateIn.close();
      } catch (Exception e) {
      }
    }
  }

  static final class BytesGenerator {
    private BytesGenerator() {
    }

    private static final byte[] CACHE = new byte[] { 0x0, 0x1, 0x2, 0x3, 0x4,
        0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF };
    private static final Random rnd = new Random(12345L);

    public static byte[] get(int size) {
      byte[] array = (byte[]) Array.newInstance(byte.class, size);
      for (int i = 0; i < size; i++)
        array[i] = CACHE[rnd.nextInt(CACHE.length - 1)];
      return array;
    }
  }
}
