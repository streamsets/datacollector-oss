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
package com.streamsets.pipeline.stage.lib.hive.cache;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.lib.hive.Errors;
import com.streamsets.pipeline.stage.lib.hive.HiveQueryExecutor;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Cache Support for TBLProperties Information.
 */
public class TBLPropertiesInfoCacheSupport
    implements HMSCacheSupport<TBLPropertiesInfoCacheSupport.TBLPropertiesInfo,
    TBLPropertiesInfoCacheSupport.TBLPropertiesInfoCacheLoader> {

  @Override
  public TBLPropertiesInfoCacheLoader newHMSCacheLoader(HiveQueryExecutor executor) {
    return new TBLPropertiesInfoCacheLoader(executor);
  }

  public static class TBLProperties {
    private boolean isExternal;
    private boolean storedAsAvro;
    //This can be later enhanced to actually return DataFormat if we need.
    String serdeLibrary;

    public TBLProperties(boolean isExternal, boolean storedAsAvro, String serdeLibrary) {
      this.isExternal = isExternal;
      this.storedAsAvro = storedAsAvro;
      this.serdeLibrary = serdeLibrary;
    }

    public boolean isExternal() {
      return isExternal;
    }

    public boolean isStoredAsAvro() {
      return storedAsAvro;
    }

    public String getSerdeLibrary() {
      return serdeLibrary;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TBLProperties that = (TBLProperties) o;

      if (isExternal != that.isExternal) return false;
      if (storedAsAvro != that.storedAsAvro) return false;
      return serdeLibrary != null ? serdeLibrary.equals(that.serdeLibrary) : that.serdeLibrary == null;

    }

    @Override
    public int hashCode() {
      int result = (isExternal ? 1 : 0);
      result = 31 * result + (storedAsAvro ? 1 : 0);
      result = 31 * result + (serdeLibrary != null ? serdeLibrary.hashCode() : 0);
      return result;
    }
  }

  public static class TBLPropertiesInfo extends HMSCacheSupport.HMSCacheInfo<TBLProperties> {
    public TBLPropertiesInfo(TBLProperties tblProperties) {
      super(tblProperties);
    }

    public boolean isExternal() {
      return state.isExternal();
    }

    public boolean isStoredAsAvro() {
      return state.isStoredAsAvro();
    }

    public String getSerdeLibrary() {
      return state.getSerdeLibrary();
    }

    @Override
    TBLProperties getDiff(TBLProperties anotherState) throws StageException {
      throw new StageException(Errors.HIVE_01, anotherState, "Invalid operation");
    }

    @Override
    void updateState(TBLProperties newState) throws StageException {
      throw new StageException(Errors.HIVE_01, newState, "Invalid operation");
    }
  }

  public class TBLPropertiesInfoCacheLoader extends HMSCacheSupport.HMSCacheLoader<TBLPropertiesInfo> {
    protected TBLPropertiesInfoCacheLoader(HiveQueryExecutor executor) {
      super(executor);
    }

    @Override
    protected TBLPropertiesInfo loadHMSCacheInfo(String qualifiedTableName) throws StageException {
      String serdeLibrary = executor.executeDescFormattedExtractSerdeLibrary(qualifiedTableName);

      Pair<Boolean, Boolean> externalAvro  = executor.executeShowTBLPropertiesQuery(qualifiedTableName);
      return new TBLPropertiesInfo(new TBLProperties(externalAvro.getLeft(), externalAvro.getRight(), serdeLibrary));
    }
  }
}
