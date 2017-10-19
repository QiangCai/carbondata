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

package org.apache.carbondata.file.mapreduce;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.BlockletHeader;
import org.apache.carbondata.format.BlockletInfo;
import org.apache.carbondata.format.FileHeader;
import org.apache.carbondata.format.MutationType;
import org.apache.carbondata.processing.loading.BadRecordsLogger;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.DataLoadProcessBuilder;
import org.apache.carbondata.processing.loading.converter.RowConverter;
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.parser.RowParser;
import org.apache.carbondata.processing.loading.parser.impl.RowParserImpl;
import org.apache.carbondata.processing.loading.steps.DataConverterProcessorStepImpl;
import org.apache.carbondata.processing.store.writer.AbstractFactDataWriter;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.spark.rdd.SparkPartitionLoader;

import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;

/**
 * Row-store record writer
 */
public class CarbonRowStoreRecordWriter extends RecordWriter {

  private final static LogService LOGGER =
      LogServiceFactory.getLogService(CarbonRowStoreRecordWriter.class.getName());

  // basic information
  private CarbonDataLoadConfiguration configuration;
  private String[] localStoreLocation = null;
  private String segmentId;
  private int taskNo;
  private CarbonTable carbonTable;

  // parser and convert row
  private RowParser rowParser;
  private RowConverter converter;
  private BadRecordsLogger badRecordLogger;
  private CarbonRow currentRow = new CarbonRow(null);

  // encode data
  private DataField[] dataFields;
  private BitSet nullBitSet;
  private boolean[] isNoDictionaryDimensionColumn;
  private int dimensionWithComplexCount;
  private int measureCount;
  private int[] measureDataTypes;
  private byte[][] buffer;
  private int rowIndex = -1;
  private long totalSizeOfBuffer = 0;
  private ByteOutputStream bos = null;
  private ObjectOutputStream oos = null;
  private Compressor compressor = null;

  // data write
  private String localFolder;
  private String fileName;
  private String localPath;
  private FileOutputStream outputStream;
  private FileChannel fileChannel;
  private boolean isFirstRow = true;
  private long timestamp = System.currentTimeMillis();

  public CarbonRowStoreRecordWriter(TaskAttemptContext job) throws IOException {
    initialize(job);
  }

  public void initialize(TaskAttemptContext job) throws IOException {
    // set basic information
    Configuration hadoopConf = job.getConfiguration();
    CarbonLoadModel carbonLoadModel = CarbonRowStoreOutputFormat.getCarbonLoadModel(hadoopConf);
    segmentId = carbonLoadModel.getSegmentId();
    carbonTable = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable();
    taskNo = TaskID.forName(hadoopConf.get("mapred.tip.id")).getId();
    carbonLoadModel.setTaskNo("" + taskNo);
    SparkPartitionLoader loader = new SparkPartitionLoader(carbonLoadModel, taskNo, null, null);
    loader.initialize();
    localStoreLocation = loader.storeLocation();
    configuration = DataLoadProcessBuilder.createConfiguration(carbonLoadModel);
  }

  private void initializeAtFirstRow() throws IOException {
    // initialize metadata
    isNoDictionaryDimensionColumn =
        CarbonDataProcessorUtil.getNoDictionaryMapping(configuration.getDataFields());
    dimensionWithComplexCount = configuration.getDimensionCount();
    measureCount = configuration.getMeasureCount();
    dataFields = configuration.getDataFields();
    measureDataTypes = new int[measureCount];
    for (int i = 0; i < measureCount; i++) {
      measureDataTypes[i] =
          dataFields[dimensionWithComplexCount + i].getColumn().getDataType().getId();
    }

    // initialize parser and converter
    rowParser = new RowParserImpl(dataFields, configuration);
    badRecordLogger = DataConverterProcessorStepImpl.createBadRecordLogger(configuration);
    converter = new RowConverterImpl(configuration.getDataFields(), configuration, badRecordLogger);
    configuration.setCardinalityFinder(converter);
    converter.initialize();

    // initialize local temp location
    if (localStoreLocation.length == 1) {
      localFolder = localStoreLocation[0];
    } else {
      localFolder = localStoreLocation[new Random().nextInt(localStoreLocation.length)];
    }
    localFolder =
        localFolder + File.separator + configuration.getTableIdentifier().getCarbonTableIdentifier()
            .getTableUniqueName() + "-" + timestamp;
    fileName =
        CarbonTablePath.getCarbonDataFileName(0, taskNo, 0, 0, "" + System.currentTimeMillis());
    localPath = localFolder + File.separator + fileName;

    // initialize
    nullBitSet = new BitSet(dataFields.length);
    outputStream = new FileOutputStream(localPath, false);
    fileChannel = outputStream.getChannel();
    isFirstRow = false;
    buffer = new byte[32000][];
    bos = new ByteOutputStream();
    oos = new ObjectOutputStream(bos);
    compressor = CompressorFactory.getInstance().getCompressor();

    // when the first row is coming, write file header
    writeFileHeader();
  }

  @Override public void write(Object key, Object value) throws IOException, InterruptedException {
    if (isFirstRow) {
      initializeAtFirstRow();
    }
    rowIndex++;
    buffer[rowIndex] = encodeRow((Object[]) value);
    totalSizeOfBuffer += buffer[rowIndex].length;
    if (rowIndex == 31999 || totalSizeOfBuffer > 64 * 1024 * 1024) {
      writeBlocklet();
    }
  }

  private void writeFileHeader() throws IOException {
    List<Integer> cardinality = new ArrayList<Integer>();
    int[] dimLensWithComplex = configuration.getCardinalityFinder().getCardinality();
    if (!configuration.isSortTable()) {
      for (int i = 0; i < dimLensWithComplex.length; i++) {
        if (dimLensWithComplex[i] != 0) {
          dimLensWithComplex[i] = Integer.MAX_VALUE;
        }
      }
    }
    List<ColumnSchema> wrapperColumnSchemaList = CarbonUtil
        .getColumnSchemaList(carbonTable.getDimensionByTableName(carbonTable.getFactTableName()),
            carbonTable.getMeasureByTableName(carbonTable.getFactTableName()));
    int[] dictionaryColumnCardinality =
        CarbonUtil.getFormattedCardinality(dimLensWithComplex, wrapperColumnSchemaList);

    List<org.apache.carbondata.format.ColumnSchema> columnSchemaList = AbstractFactDataWriter
        .getColumnSchemaListAndCardinality(cardinality, dictionaryColumnCardinality,
            wrapperColumnSchemaList);
    FileHeader fileHeader = CarbonMetadataUtil.getFileHeader(true, columnSchemaList, timestamp);
    fileHeader.setIs_footer_present(false);
    fileHeader.setIs_splitable(true);
    fileHeader.setSync_marker(CarbonRowStoreOutputFormat.SYNC_MARKER);
    byte[] headerBytes = CarbonUtil.getByteArray(fileHeader);
    fileChannel.write(ByteBuffer.wrap(headerBytes));
  }

  private void writeBlocklet() throws IOException {
    if (rowIndex == -1) {
      return;
    }
    BlockletInfo blockletInfo = new BlockletInfo();
    blockletInfo.setNum_rows(rowIndex + 1);
    BlockletHeader blockletHeader = new BlockletHeader();
    byte[] compressed = compressBlockletData();
    blockletHeader.setBlocklet_length(compressed.length);
    blockletHeader.setMutation(MutationType.INSERT);
    blockletHeader.setBlocklet_info(blockletInfo);
    byte[] headerBytes = CarbonUtil.getByteArray(blockletHeader);
    fileChannel.write(ByteBuffer.wrap(headerBytes));
    fileChannel.write(ByteBuffer.wrap(compressed));
    fileChannel.write(ByteBuffer.wrap(CarbonRowStoreOutputFormat.SYNC_MARKER));

    // reset data
    for (int i = 0; i <= rowIndex; i++) {
      buffer[i] = null;
    }
    rowIndex = -1;
    totalSizeOfBuffer = 0;
  }

  private byte[] encodeRow(Object[] row) throws IOException {
    // parse and convert row
    currentRow.setData(rowParser.parseRow(row));
    converter.convert(currentRow);

    // null bit set
    nullBitSet.clear();
    for (int i = 0; i < dataFields.length; i++) {
      if (null == currentRow.getObject(i)) {
        nullBitSet.set(i);
      }
    }
    oos.reset();
    bos.reset();
    oos.write(nullBitSet.toByteArray());
    int dimCount = 0;
    Object value;

    for (; dimCount < isNoDictionaryDimensionColumn.length; dimCount++) {
      value = currentRow.getObject(dimCount);
      if (value != null) {
        if (isNoDictionaryDimensionColumn[dimCount]) {
          byte[] col = (byte[]) value;
          oos.writeShort(col.length);
          oos.write(col);
        } else {
          oos.writeInt((int) value);
        }
      }
    }

    for (; dimCount < dimensionWithComplexCount; dimCount++) {
      value = currentRow.getObject(dimCount);
      if (value != null) {
        byte[] col = (byte[]) value;
        oos.writeShort(col.length);
        oos.write(col);
      }
    }
    int dataType;
    for (int msrCount = 0; msrCount < measureCount; msrCount++) {
      value = currentRow.getObject(dimCount + msrCount);
      dataType = measureDataTypes[msrCount];
      if (dataType == DataTypes.BOOLEAN_TYPE_ID) {
        oos.writeBoolean((boolean) value);
      } else if (dataType == DataTypes.SHORT_TYPE_ID) {
        oos.writeShort((Short) value);
      } else if (dataType == DataTypes.INT_TYPE_ID) {
        oos.writeInt((Integer) value);
      } else if (dataType == DataTypes.LONG_TYPE_ID) {
        oos.writeLong((Long) value);
      } else if (dataType == DataTypes.DOUBLE_TYPE_ID) {
        oos.writeDouble((Double) value);
      } else if (dataType == DataTypes.DECIMAL_TYPE_ID) {
        BigDecimal val = (BigDecimal) value;
        byte[] bigDecimalInBytes = DataTypeUtil.bigDecimalToByte(val);
        oos.writeInt(bigDecimalInBytes.length);
        oos.write(bigDecimalInBytes);
      } else {
        throw new IllegalArgumentException(
            "unsupported data type:" + dataFields[dimCount + msrCount].getColumn().getDataType()
                .getName());
      }
    }
    oos.flush();
    return bos.getBytes();
  }

  private byte[] compressBlockletData() {
    // use snappy to compress buffer data
    if (rowIndex == 0) {
      return compressor.compressByte(buffer[rowIndex]);
    } else if (rowIndex > 0) {
      byte[] data = new byte[(int) totalSizeOfBuffer];
      int offset = 0;
      for (byte[] row : buffer) {
        System.arraycopy(row, 0, data, offset, row.length);
        offset += row.length;
      }
      return compressor.compressByte(data);
    }
    return null;
  }

  private void copyLocalFileToCarbonStorePath() {
    CarbonTablePath tablePath =
        CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier());
    String targetFolder = tablePath.getStreamingTempDir("0", segmentId);
    LOGGER.info("Copying " + localPath + " --> " + targetFolder);
    long copyStartTime = System.currentTimeMillis();
    try {
      CarbonFile localCarbonFile =
          FileFactory.getCarbonFile(localPath, FileFactory.getFileType(localPath));
      String targetFilePath = targetFolder + File.separator + fileName;

      DataOutputStream dataOutputStream = null;
      DataInputStream dataInputStream = null;
      try {
        dataOutputStream = FileFactory
            .getDataOutputStream(targetFilePath, FileFactory.getFileType(targetFilePath),
                CarbonCommonConstants.BYTEBUFFER_SIZE, 128 * 1024 * 1024);
        dataInputStream = FileFactory
            .getDataInputStream(localPath, FileFactory.getFileType(localPath),
                CarbonCommonConstants.BYTEBUFFER_SIZE);
        IOUtils.copyBytes(dataInputStream, dataOutputStream, CarbonCommonConstants.BYTEBUFFER_SIZE);
      } finally {
        CarbonUtil.closeStream(dataInputStream);
        CarbonUtil.closeStream(dataOutputStream);
      }
    } catch (IOException e) {
      throw new CarbonDataWriterException(
          "Problem while copying file from local store to carbon store", e);
    }
    LOGGER.info(
        "Total copy time (ms) to copy file " + fileName + " is " + (System.currentTimeMillis()
            - copyStartTime));
  }

  @Override public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    // write remain buffer data to temp file
    writeBlocklet();

    copyLocalFileToCarbonStorePath();

    // close resource
    if (outputStream != null) {
      try {
        outputStream.close();
      } catch (Exception ex) {
        LOGGER.error(ex, "Failed to close output stream");
      } finally {
        outputStream = null;
        fileChannel = null;
      }
    }
  }

}