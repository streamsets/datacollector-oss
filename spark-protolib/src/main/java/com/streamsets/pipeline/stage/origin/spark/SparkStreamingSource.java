package com.streamsets.pipeline.stage.origin.spark;

import java.util.List;

/**
 * Spark streaming source which should be implemented by any source which has
 * spark support
 *
 */
public interface SparkStreamingSource {

  /**
   * Writes batch of data to the source
   * @param batch
   * @throws InterruptedException
   */
  <T> void put(List<T> batch) throws InterruptedException;

  /**
   * Return the no of records produced by this source
   * @return
   */
  long getRecordsProduced();

}
