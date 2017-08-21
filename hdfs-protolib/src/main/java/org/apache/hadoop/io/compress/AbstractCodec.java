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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class AbstractCodec implements Configurable, CompressionCodec {
  public static final String SDC= "SDC";

  private final Class<? extends Compressor> compressorClass;
  private final Class<? extends Decompressor> decompressorClass;
  private final String defaultExtension;
  private Configuration conf;
  private int bufferSize = -1;

  public AbstractCodec(Class<? extends Compressor> compressorClass, Class<? extends Decompressor> decompressorClass,
      String defaultExtension) {
    this.compressorClass = compressorClass;
    this.decompressorClass = decompressorClass;
    this.defaultExtension = defaultExtension;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    init(conf);
    if (bufferSize == -1) {
      throw new IllegalStateException("The bufferSize has not been set by init()");
    }
  }

  // it must call setBufferSize
  protected abstract void init(Configuration conf);

  protected void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  protected int getBufferSize() {
    return bufferSize;
  }

  protected abstract int getCompressionOverhead();

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    return createOutputStream(out, createCompressor());
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
    return new BlockCompressorStream(out, compressor, getBufferSize(), getCompressionOverhead());
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return compressorClass;
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    return createInputStream(in, createDecompressor());
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
    return new BlockDecompressorStream(in, decompressor, getBufferSize());
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return decompressorClass;
  }

  @Override
  public String getDefaultExtension() {
    return defaultExtension;
  }

}
