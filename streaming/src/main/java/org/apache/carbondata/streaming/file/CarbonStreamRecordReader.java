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

package org.apache.carbondata.streaming.file;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import org.apache.carbondata.hadoop.CarbonMultiBlockSplit;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.streaming.util.StreamingUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Stream record reader
 */
public class CarbonStreamRecordReader extends RecordReader {

  // metadata
  private CarbonTable carbonTable;
  private DataField[] dataFields;
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
  private boolean[] isInProjection;
  private CarbonColumn[] projection;
  private int[] projectionMap;
  private FilterResolverIntf filter;

  // decode data
  private Object[] currentValue;
  private BitSet allNonNull;
  private boolean[] isNoDictColumn;
  private DirectDictionaryGenerator[] directDictionaryGenerators;
  private CacheProvider cacheProvider;
  private Cache<DictionaryColumnUniqueIdentifier, Dictionary> cache;
  private GenericQueryType[] queryTypes;

  @Override public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    if (split instanceof CarbonMultiBlockSplit) {
      fileSplit = ((CarbonMultiBlockSplit) split).getAllSplits().get(0);
    } else {
      fileSplit = (FileSplit) split;
    }

    hadoopConf = context.getConfiguration();
    CarbonTableInputFormat format = new CarbonTableInputFormat<Object>();
    model = format.getQueryModel(split, context);
    carbonTable = model.getTable();

    List<CarbonDimension> dimensions =
        carbonTable.getDimensionByTableName(carbonTable.getFactTableName());
    dimensionCount = dimensions.size();
    List<CarbonMeasure> measures =
        carbonTable.getMeasureByTableName(carbonTable.getFactTableName());
    measureCount = measures.size();
    List<DataField> dataFieldList = new ArrayList<>();
    List<DataField> complexDataFields = new ArrayList<>();
    for (CarbonColumn column : dimensions) {
      DataField dataField = new DataField(column);
      if (column.isComplex()) {
        complexDataFields.add(dataField);
      } else {
        dataFieldList.add(dataField);
      }
    }
    dataFieldList.addAll(complexDataFields);
    for (CarbonColumn column : measures) {
      if (!(column.getColName().equals("default_dummy_measure"))) {
        dataFieldList.add(new DataField(column));
      }
    }
    dataFields = dataFieldList.toArray(new DataField[dataFieldList.size()]);

    isNoDictColumn = CarbonDataProcessorUtil.getNoDictionaryMapping(dataFields);

    directDictionaryGenerators = new DirectDictionaryGenerator[dataFields.length];
    for (int i = 0; i < dataFields.length; i++) {
      if (dataFields[i].getColumn().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        directDictionaryGenerators[i] = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(dataFields[i].getColumn().getDataType());
      }
    }
    measureDataTypes = new int[measureCount];
    for (int i = 0; i < measureCount; i++) {
      measureDataTypes[i] = dataFields[dimensionCount + i].getColumn().getDataType().getId();
    }

    allNonNull = new BitSet(dataFields.length);

    projection = model.getProjectionColumns();

    projectionMap = new int[projection.length];
    isInProjection = new boolean[dataFields.length];
    for (int i = 0; i < projection.length; i++) {
      for (int j = 0; j < dataFields.length; j++) {
        if (dataFields[j].getColumn().getColName().equals(projection[i].getColName())) {
          projectionMap[i] = j;
          isInProjection[j] = true;
          break;
        }
      }
    }

    filter = model.getFilterExpressionResolverTree();

  }

  private byte[] getSyncMarker(String filePath) throws IOException {
    CarbonHeaderReader headerReader = new CarbonHeaderReader(filePath);
    FileHeader header = headerReader.readHeader();
    header.getColumn_schema();
    return header.getSync_marker();
  }

  private void initializeAtFirstRow() throws IOException {
    currentValue = new Object[dataFields.length];

    Path file = fileSplit.getPath();

    byte[] syncMarker = getSyncMarker(file.toUri().getPath());

    FileSystem fs = file.getFileSystem(hadoopConf);

    int bufferSize = Integer.parseInt(hadoopConf.get(CarbonStreamInputFormat.READ_BUFFER_SIZE,
        CarbonStreamInputFormat.READ_BUFFER_SIZE_DEFAULT));

    FSDataInputStream fileIn = fs.open(file, bufferSize);
    fileIn.seek(fileSplit.getStart());
    bis = new BlockletInputStream(syncMarker, fileIn, fileSplit.getLength());

    cacheProvider = CacheProvider.getInstance();
    cache = cacheProvider.createCache(CacheType.FORWARD_DICTIONARY, carbonTable.getStorePath());
    queryTypes = StreamingUtil.getComplexDimensions(carbonTable, dataFields, cache);
  }

  @Override public boolean nextKeyValue() throws IOException, InterruptedException {
    if (isFirstRow) {
      isFirstRow = false;
      initializeAtFirstRow();
    }
    if (bis.hasNext()) {
      if (null == filter) {
        readRow();
        return true;
      } else {

      }
    }
    return false;
  }

  private void readRow() {
    bis.nextRow();
    short nullLen = bis.readShort();
    BitSet nullBitSet = allNonNull;
    if (0 != nullLen) {
      nullBitSet = BitSet.valueOf(bis.readBytes(nullLen));
    }
    int dimCount = 0;
    // primitive type dimension
    for (; dimCount < isNoDictColumn.length; dimCount++) {
      if (isInProjection[dimCount]) {
        if (nullBitSet.get(dimCount)) {
          currentValue[dimCount] = null;
        } else {
          if (isNoDictColumn[dimCount]) {
            int v = bis.readShort();
            byte[] b = bis.readBytes(v);
            currentValue[dimCount] = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(b,
                dataFields[dimCount].getColumn().getDataType());
          } else if (null != directDictionaryGenerators[dimCount]) {
            currentValue[dimCount] =
                directDictionaryGenerators[dimCount].getValueFromSurrogate(bis.readInt());
          } else {
            currentValue[dimCount] = bis.readInt();
          }
        }
      }
    }
    // complex type dimension
    for (; dimCount < dimensionCount; dimCount++) {
      if (isInProjection[dimCount]) {
        if (nullBitSet.get(dimCount)) {
          currentValue[dimCount] = null;
        } else {
          short v = bis.readShort();
          currentValue[dimCount] = queryTypes[dimCount]
              .getDataBasedOnDataTypeFromSurrogates(ByteBuffer.wrap(bis.readBytes(v)));
        }
      }
    }
    // measure
    int dataType;
    for (int msrCount = 0; msrCount < measureCount; msrCount++, dimCount++) {
      if (isInProjection[dimCount]) {
        if (nullBitSet.get(dimCount)) {
          currentValue[dimCount] = null;
        } else {
          dataType = measureDataTypes[msrCount];
          if (dataType == DataTypes.BOOLEAN_TYPE_ID) {
            currentValue[dimCount] = bis.readBoolean();
          } else if (dataType == DataTypes.SHORT_TYPE_ID) {
            currentValue[dimCount] = bis.readShort();
          } else if (dataType == DataTypes.INT_TYPE_ID) {
            currentValue[dimCount] = bis.readInt();
          } else if (dataType == DataTypes.LONG_TYPE_ID) {
            currentValue[dimCount] = bis.readLong();
          } else if (dataType == DataTypes.DOUBLE_TYPE_ID) {
            currentValue[dimCount] = bis.readDouble();
          } else if (dataType == DataTypes.DECIMAL_TYPE_ID) {
            int len = bis.readShort();
            currentValue[dimCount] = DataTypeUtil.byteToBigDecimal(bis.readBytes(len));
          }
        }
      }
    }
  }

  @Override public Object getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override public Object getCurrentValue() throws IOException, InterruptedException {
    Object[] result = new Object[projection.length];
    for (int i = 0; i < projection.length; i++) {
      result[i] = currentValue[projectionMap[i]];
    }
    return result;
  }

  @Override public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override public void close() throws IOException {
    bis.close();
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
    private int rowNums = -1;
    private int rowIndex = -1;

    BlockletInputStream(byte[] syncMarker, InputStream in, long limit) {
      this.syncMarker = syncMarker;
      this.syncLen = syncMarker.length;
      this.syncBuffer = new byte[syncMarker.length];
      this.in = in;
      this.limitStart = limit;
      this.limitEnd = limitStart + syncLen;
    }

    private void ensureCapacity(int capacity) {
      if (capacity > buffer.length) {
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
      for (int i = 0; i < limitStart; i++) {
        int j = 0;
        for (; j < syncLen; j++) {
          if (syncMarker[j] != syncBuffer[(i + j) % syncLen]) break;
        }
        if (syncLen == j) {
          return true;
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
    boolean hasNextBlocklet() throws IOException {
      if (pos >= limitEnd) {
        return false;
      }
      if (isAlreadySync) {
        in.read(syncBuffer);
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
      readBlockletData();
      return rowNums > 0;
    }

    boolean hasNext() throws IOException {
      if (rowIndex < rowNums) {
        return true;
      } else {
        return hasNextBlocklet();
      }
    }

    void nextRow() {
      rowIndex++;
      offset = 0;
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
      return (short) (((buffer[offset++]) << 8) +
          ((buffer[offset++] & 255) << 0));
    }

    int readInt() {
      return (((buffer[offset]) << 24) +
          ((buffer[offset++] & 255) << 16) +
          ((buffer[offset++] & 255) << 8) +
          ((buffer[offset++] & 255) << 0));
    }

    long readLong() {
      return (((long) buffer[offset++] << 56) +
          ((long) (buffer[offset++] & 255) << 48) +
          ((long) (buffer[offset++] & 255) << 40) +
          ((long) (buffer[offset++] & 255) << 32) +
          ((long) (buffer[offset++] & 255) << 24) +
          ((buffer[offset++] & 255) << 16) +
          ((buffer[offset++] & 255) << 8) +
          ((buffer[offset++] & 255) << 0));
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
