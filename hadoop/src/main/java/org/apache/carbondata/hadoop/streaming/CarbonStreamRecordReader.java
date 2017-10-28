/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.hadoop.streaming;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.format.BlockletHeader;
import org.apache.carbondata.format.FileHeader;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.CarbonTypeUtil;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Stream record reader
 */
public class CarbonStreamRecordReader extends RecordReader<Void, Object>{

  // metadata
  private CarbonTable carbonTable;
  private CarbonColumn[] carbonColumns;
  private int[] measureDataTypes;
  private int dimensionCount;
  private int measureCount;

  // input
  private FileSplit fileSplit;
  private Configuration hadoopConf;
  private BlockletInputStream bis;
  private boolean isFirstRow = true;

  // query configuration
  private QueryModel model;
  private CarbonColumn[] projection;
  private boolean[] isInProjection;
  private int[] projectionMap;
  private FilterResolverIntf filter;

  // decode data
  private Object[] originalValue;
  private Object[] convertedValue;
  private BitSet allNonNull;
  private boolean[] isNoDictColumn;
  private DirectDictionaryGenerator[] directDictionaryGenerators;

  private CacheProvider cacheProvider;
  private Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache;
  private GenericQueryType[] queryTypes;

  // vectorized reader
  private StructType outputSchema;
  private ColumnarBatch columnarBatch;
  private boolean isFinished = false;

  @Override public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    if (split instanceof CarbonInputSplit) {
      fileSplit = (CarbonInputSplit) split;
    } else if (split instanceof CarbonMultiBlockSplit) {
      fileSplit = ((CarbonMultiBlockSplit) split).getAllSplits().get(0);
    } else {
      fileSplit = (FileSplit) split;
    }

    hadoopConf = context.getConfiguration();
    if (model == null) {
      CarbonTableInputFormat format = new CarbonTableInputFormat<Object>();
      model = format.getQueryModel(split, context);
    }
    carbonTable = model.getTable();

    List<CarbonDimension> dimensions =
        carbonTable.getDimensionByTableName(carbonTable.getFactTableName());
    dimensionCount = dimensions.size();
    List<CarbonMeasure> measures =
        carbonTable.getMeasureByTableName(carbonTable.getFactTableName());
    measureCount = measures.size();
    List<CarbonColumn> columns =
        carbonTable.getStreamStorageOrderColumn(carbonTable.getFactTableName());
    carbonColumns = columns.toArray(new CarbonColumn[columns.size()]);

    isNoDictColumn = CarbonDataProcessorUtil.getNoDictionaryMapping(carbonColumns);

    directDictionaryGenerators = new DirectDictionaryGenerator[carbonColumns.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      if (carbonColumns[i].hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        directDictionaryGenerators[i] = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(carbonColumns[i].getDataType());
      }
    }
    measureDataTypes = new int[measureCount];
    for (int i = 0; i < measureCount; i++) {
      measureDataTypes[i] = carbonColumns[dimensionCount + i].getDataType().getId();
    }

    allNonNull = new BitSet(carbonColumns.length);

    projection = model.getProjectionColumns();

    isInProjection = new boolean[carbonColumns.length];
    projectionMap = new int[projection.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      for (int j = 0; j < projection.length; j++) {
        if (carbonColumns[i].getColName().equals(projection[j].getColName())) {
          isInProjection[i] = true;
          projectionMap[j] = i;
          break;
        }
      }
    }
    filter = model.getFilterExpressionResolverTree();
  }

  public void setQueryModel(QueryModel model) {
    this.model = model;
  }

  private byte[] getSyncMarker(String filePath) throws IOException {
    CarbonHeaderReader headerReader = new CarbonHeaderReader(filePath);
    FileHeader header = headerReader.readHeader();
    header.getColumn_schema();
    return header.getSync_marker();
  }

  private void initializeAtFirstRow() throws IOException {
    originalValue = new Object[carbonColumns.length];
    convertedValue = new Object[carbonColumns.length];

    Path file = fileSplit.getPath();

    byte[] syncMarker = getSyncMarker(file.toUri().getPath());

    FileSystem fs = file.getFileSystem(hadoopConf);

    int bufferSize = Integer.parseInt(hadoopConf.get(CarbonStreamInputFormat.READ_BUFFER_SIZE,
        CarbonStreamInputFormat.READ_BUFFER_SIZE_DEFAULT));

    FSDataInputStream fileIn = fs.open(file, bufferSize);
    fileIn.seek(fileSplit.getStart());
    bis = new BlockletInputStream(syncMarker, fileIn, fileSplit.getLength(),
        fileSplit.getStart() == 0);

    cacheProvider = CacheProvider.getInstance();
    cache = cacheProvider.createCache(CacheType.FORWARD_DICTIONARY, carbonTable.getStorePath());
    queryTypes = CarbonInputFormatUtil.getComplexDimensions(carbonTable, carbonColumns, cache);

    outputSchema = new StructType(CarbonTypeUtil.convertCarbonSchemaToSparkSchema(projection));
  }

  @Override public boolean nextKeyValue() throws IOException, InterruptedException {
    if (isFirstRow) {
      isFirstRow = false;
      initializeAtFirstRow();
    }

    if (isFinished) {
      return false;
    }

    boolean hasNext = bis.nextBlocklet();
    if (hasNext) {
      fillColumnBatch();
    } else {
      isFinished = true;
    }
    return hasNext;
  }

  private void fillColumnBatch() throws IOException {
    columnarBatch = ColumnarBatch.allocate(outputSchema, MemoryMode.OFF_HEAP, bis.rowNums);
    int rowNum = 0;
    while (bis.hasNext()) {
      readRowFromStream();
      fillRowColumnBatch(rowNum++);
    }
    columnarBatch.setNumRows(rowNum);
  }

  private void readRowFromStream() {
    bis.nextRow();
    short nullLen = bis.readShort();
    BitSet nullBitSet = allNonNull;
    if (nullLen > 0) {
      nullBitSet = BitSet.valueOf(bis.readBytes(nullLen));
    }
    int dimCount = 0;
    // primitive type dimension
    for (; dimCount < isNoDictColumn.length; dimCount++) {
      if (isInProjection[dimCount]) {
        if (nullBitSet.get(dimCount)) {
          originalValue[dimCount] = null;
          convertedValue[dimCount] = null;
        } else {
          if (isNoDictColumn[dimCount]) {
            int v = bis.readShort();
            byte[] b = bis.readBytes(v);
            originalValue[dimCount] = b;
            convertedValue[dimCount] = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(b,
                carbonColumns[dimCount].getDataType());
          } else if (null != directDictionaryGenerators[dimCount]) {
            int v = bis.readInt();
            originalValue[dimCount] = v;
            convertedValue[dimCount] =
                directDictionaryGenerators[dimCount].getValueFromSurrogate(v);
          } else {
            originalValue[dimCount] =bis.readInt();
            convertedValue[dimCount] = originalValue[dimCount];
          }
        }
      }
    }
    // complex type dimension
    for (; dimCount < dimensionCount; dimCount++) {
      if (isInProjection[dimCount]) {
        if (nullBitSet.get(dimCount)) {
          originalValue[dimCount] = null;
          convertedValue[dimCount] = null;
        } else {
          short v = bis.readShort();
          byte[] b = bis.readBytes(v);
          originalValue[dimCount] = b;
          convertedValue[dimCount] = queryTypes[dimCount]
              .getDataBasedOnDataTypeFromSurrogates(ByteBuffer.wrap(b));
        }
      }
    }
    // measure
    int dataType;
    for (int msrCount = 0; msrCount < measureCount; msrCount++, dimCount++) {
      if (isInProjection[dimCount]) {
        if (nullBitSet.get(dimCount)) {
          originalValue[dimCount] = null;
          convertedValue[dimCount] = null;
        } else {
          dataType = measureDataTypes[msrCount];
          if (dataType == DataTypes.BOOLEAN_TYPE_ID) {
            originalValue[dimCount] = bis.readBoolean();
            convertedValue[dimCount] = originalValue[dimCount];
          } else if (dataType == DataTypes.SHORT_TYPE_ID) {
            originalValue[dimCount] = bis.readShort();
            convertedValue[dimCount] = originalValue[dimCount];
          } else if (dataType == DataTypes.INT_TYPE_ID) {
            originalValue[dimCount] = bis.readInt();
            convertedValue[dimCount] = originalValue[dimCount];
          } else if (dataType == DataTypes.LONG_TYPE_ID) {
            originalValue[dimCount] = bis.readLong();
            convertedValue[dimCount] = originalValue[dimCount];
          } else if (dataType == DataTypes.DOUBLE_TYPE_ID) {
            originalValue[dimCount] = bis.readDouble();
            convertedValue[dimCount] = originalValue[dimCount];
          } else if (dataType == DataTypes.DECIMAL_TYPE_ID) {
            int len = bis.readShort();
            originalValue[dimCount] = DataTypeUtil.byteToBigDecimal(bis.readBytes(len));
            convertedValue[dimCount] = originalValue[dimCount];
          }
        }
      }
    }
  }

  private void fillRowColumnBatch(int rowId) {
    for (int i = 0; i < projection.length; i++) {
      Object value = convertedValue[projectionMap[i]];
      ColumnVector col = columnarBatch.column(i);
      DataType t = col.dataType();
      if (null == value) {
        col.putNull(rowId);
      } else {
        if (t == org.apache.spark.sql.types.DataTypes.BooleanType) {
          col.putBoolean(rowId, (boolean)value);
        } else if (t == org.apache.spark.sql.types.DataTypes.ByteType) {
          col.putByte(rowId, (byte) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.ShortType) {
          col.putShort(rowId, (short) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.IntegerType) {
          col.putInt(rowId, (int) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.LongType) {
          col.putLong(rowId, (long) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.FloatType) {
          col.putFloat(rowId, (float) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.DoubleType) {
          col.putDouble(rowId, (double) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.StringType) {
          UTF8String v = (UTF8String) value;
          col.putByteArray(rowId, v.getBytes());
        } else if (t instanceof DecimalType) {
          DecimalType dt = (DecimalType)t;
          Decimal d = (Decimal) value;
          if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
            col.putInt(rowId, (int)d.toUnscaledLong());
          } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
            col.putLong(rowId, d.toUnscaledLong());
          } else {
            final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
            byte[] bytes = integer.toByteArray();
            col.putByteArray(rowId, bytes, 0, bytes.length);
          }
        } else if (t instanceof CalendarIntervalType) {
          CalendarInterval c = (CalendarInterval) value;
          col.getChildColumn(0).putInt(rowId, c.months);
          col.getChildColumn(1).putLong(rowId, c.microseconds);
        } else if (t instanceof DateType) {
          col.putInt(rowId, (int) value);
        } else if (t instanceof TimestampType) {
          col.putLong(rowId, (long) value);
        }
      }
    }
  }

  @Override public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override public Object getCurrentValue() throws IOException, InterruptedException {
    return columnarBatch;
  }

  @Override public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override public void close() throws IOException {
    if (null != bis) {
      bis.close();
    }
    if (null != columnarBatch) {
      columnarBatch.close();
    }
  }

  static class BlockletInputStream {

    private byte[] buffer;
    private int offset;
    private final byte[] syncMarker;
    private final byte[] syncBuffer;
    private final int syncLen;
    private long pos = 0;
    private final InputStream in;
    private final long limitStart;
    private final long limitEnd;
    private boolean isAlreadySync = false;
    private Compressor compressor = CompressorFactory.getInstance().getCompressor();
    private int rowNums = 0;
    private int rowIndex = 0;
    private boolean isHeaderPresent;

    BlockletInputStream(byte[] syncMarker, InputStream in, long limit, boolean isHeaderPresent) {
      this.syncMarker = syncMarker;
      this.syncLen = syncMarker.length;
      this.syncBuffer = new byte[syncMarker.length];
      this.in = in;
      this.limitStart = limit;
      this.limitEnd = limitStart + syncLen;
      this.isHeaderPresent = isHeaderPresent;
    }

    private void ensureCapacity(int capacity) {
      if (buffer == null || capacity > buffer.length) {
        buffer = new byte[capacity];
      }
    }

    /**
     * find the first position of sync_marker in input stream
     */
    private boolean sync() throws IOException {
      int len = in.read(syncBuffer);
      if (len < syncLen) {
        return false;
      }
      pos += syncLen;
      boolean skipHeader = false;
      for (int i = 0; i < limitStart; i++) {
        int j = 0;
        for (; j < syncLen; j++) {
          if (syncMarker[j] != syncBuffer[(i + j) % syncLen]) break;
        }
        if (syncLen == j) {
          if (isHeaderPresent) {
            if (skipHeader) {
              return true;
            } else {
              skipHeader = true;
            }
          } else {
            return true;
          }
        }
        int value = in.read();
        if (-1 == value) {
          return false;
        }
        syncBuffer[i % syncLen] = (byte) value;
        pos++;
      }
      return false;
    }

    BlockletHeader readBlockletHeader() throws IOException {
      int len = readIntFromStream();
      byte[] b = new byte[len];
      readBytesFromStream(b);
      return CarbonUtil.readBlockletHeader(b);
    }

    void readBlockletData() throws IOException {
      int len = readIntFromStream();
      byte[] b = new byte[len];
      readBytesFromStream(b);
      compressor.rawUncompress(b, buffer);
    }

    /**
     * read blocklet one by one
     */
    boolean nextBlocklet() throws IOException {
      if (pos >= limitEnd) {
        return false;
      }
      if (isAlreadySync) {
        int v = in.read(syncBuffer);
        if (v < syncLen) {
          return false;
        }
        pos += syncLen;
      } else {
        isAlreadySync = true;
        if (!sync()) {
          return false;
        }
      }
      BlockletHeader header = readBlockletHeader();
      ensureCapacity(header.getBlocklet_length());
      rowNums = header.getBlocklet_info().getNum_rows();
      rowIndex = 0;
      offset = 0;
      readBlockletData();
      return rowNums > 0;
    }

    boolean hasNext() throws IOException {
      return rowIndex < rowNums;
    }

    void nextRow() {
      rowIndex++;
    }

    void close() {
      CarbonUtil.closeStreams(in);
    }

    int readIntFromStream() throws IOException {
      int ch1 = in.read();
      int ch2 = in.read();
      int ch3 = in.read();
      int ch4 = in.read();
      if ((ch1 | ch2 | ch3 | ch4) < 0) throw new EOFException();
      pos += 4;
      return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    void readBytesFromStream(byte[] b) throws IOException {
      int len = in.read(b, 0, b.length);
      if (len < b.length) {
        throw new EOFException();
      }
      pos += b.length;
    }

    boolean readBoolean() {
      return (buffer[offset++]) != 0;
    }

    short readShort() {
      short v =  (short) ((buffer[offset + 1] & 255) +
          ((buffer[offset]) << 8));
      offset += 2;
      return v;
    }

    int readInt() {
      int v = ((buffer[offset + 3] & 255) +
          ((buffer[offset + 2] & 255) << 8) +
          ((buffer[offset + 1] & 255) << 16) +
          ((buffer[offset]) << 24));
      offset += 4;
      return v;
    }

    long readLong() {
      long v = ((long)(buffer[offset + 7] & 255)) +
          ((long) (buffer[offset + 6] & 255) << 8) +
          ((long) (buffer[offset + 5] & 255) << 16) +
          ((long) (buffer[offset + 4] & 255) << 24) +
          ((long) (buffer[offset + 3] & 255) << 32) +
          ((long) (buffer[offset + 2] & 255) << 40) +
          ((long) (buffer[offset + 1] & 255) << 48) +
          ((long) (buffer[offset]) << 56);
      offset += 8;
      return v;
    }

    double readDouble() {
      return Double.longBitsToDouble(readLong());
    }

    byte[] readBytes(int len) {
      byte[] b = new byte[len];
      System.arraycopy(buffer, offset, b, 0, len);
      offset += len;
      return b;
    }
  }
}
