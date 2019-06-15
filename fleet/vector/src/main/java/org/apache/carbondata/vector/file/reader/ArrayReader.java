package org.apache.carbondata.vector.file.reader;

import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.vector.file.vector.ArrayVector;

import org.apache.hadoop.conf.Configuration;

/**
 * interface to read array data file
 */
@InterfaceAudience.Internal
@InterfaceStability.Evolving
public interface ArrayReader extends AutoCloseable {

  /**
   * open the reader and init the configuration
   * @param inputFolder
   * @param hadoopConf
   */
  void open(String inputFolder, Configuration hadoopConf) throws IOException;

  /**
   * close the reader and release resources
   */
  void close() throws IOException;
}
