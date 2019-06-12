package org.apache.carbondata.vector.table;

import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;

import java.io.File;

/**
 * file layout and file name
 */
public class VectorTablePath {

  /**
   * name of column data file
   * @param column
   * @return
   */
  public static String getColumnFileName(CarbonColumn column) {
    return column.getColName() + ".array";

  }

  /**
   * offset of each value in column data
   * @param column
   * @return
   */
  public static String getOffsetFileName(CarbonColumn column) {
    return column.getColName() + ".offset";
  }

  /**
   * file path of column file
   * @param folderPath
   * @param column
   * @return
   */
  public static String getColumnFilePath(String folderPath, CarbonColumn column) {
    return folderPath + File.separator + getColumnFileName(column);
  }

  /**
   * file path of offset file
   * @param folderPath
   * @param column
   * @return
   */
  public static String getOffsetFilePath(String folderPath, CarbonColumn column) {
    return folderPath + File.separator + getOffsetFileName(column);
  }


  /**
   * name of column data file
   * @param column
   * @return
   */
  public static String getColumnFolderName(CarbonColumn column) {
    return column.getColName();

  }

  public static String getComplexFolderPath(String folderPath, CarbonColumn column) {
    return folderPath + File.separator + getColumnFolderName(column);
  }

}
