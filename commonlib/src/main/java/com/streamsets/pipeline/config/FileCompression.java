/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

@GenerateResourceBundle
public enum FileCompression implements Label {
  NONE("Uncompressed", new NoneOpener()),
  ZIP("Zip", new ZipOpener()),
  GZIP("GZip", new GZipOpener()),
  AUTOMATIC("By Extension (zip, gz, gzip)", new AutomaticOpener())
  ;

  private final String label;
  private final Opener opener;

  FileCompression(String label, Opener opener) {
    this.label = label;
    this.opener = opener;
  }

  @Override
  public String getLabel() {
    return label;
  }

  public InputStream open(File file) throws IOException {
    return opener.open(file);
  }

  private interface Opener {

    public InputStream open(File file) throws IOException;

  }

  private static class NoneOpener implements Opener {

    @Override
    public InputStream open(File file) throws IOException {
      return new FileInputStream(file);
    }
  }

  private static class ZipOpener implements Opener {

    @Override
    public InputStream open(File file) throws IOException {
      ZipInputStream zip = new ZipInputStream(new FileInputStream(file));
      ZipEntry entry = zip.getNextEntry();
      if (entry != null) {
        return zip;
      } else {
        throw new IOException(Utils.format("Zip File '{}' has more than one file", file));
      }
    }
  }

  private static class GZipOpener implements Opener {

    @Override
    public InputStream open(File file) throws IOException {
      return new GZIPInputStream(new FileInputStream(file));
    }
  }

  private static class AutomaticOpener implements Opener {
    @Override
    public InputStream open(File file) throws IOException {
      InputStream is;
      String name = file.getName();
      int idx = name.lastIndexOf(".");
      String extension = (idx > -1) ? name.substring(idx + 1) : "";
      switch (extension.toLowerCase()) {
        case "zip":
          is = ZIP.open(file);
          break;
        case "gzip":
        case "gz":
          is = GZIP.open(file);
          break;
        default:
          is = NONE.open(file);
      }
      return is;
    }
  }

}
