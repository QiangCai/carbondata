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
package org.apache.carbon.flink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.sdk.file.Schema;
import org.apache.hadoop.conf.Configuration;

final class CarbonS3WriterFactory extends CarbonWriterFactory {

    CarbonS3WriterFactory(
        final String databaseName,
        final String tableName,
        final String tablePath,
        final Properties tableProperties,
        final Properties writerProperties,
        final Properties carbonProperties
    ) {
        super(databaseName, tableName, tablePath, tableProperties, writerProperties, carbonProperties);
    }

    @Override
    protected CarbonWriter create(
        final CarbonTable table,
        final Properties tableProperties,
        final Properties writerProperties
    ) throws IOException {
        final String outputRootPathLocal = writerProperties.getProperty(CarbonS3Property.DATA_LOCAL_PATH);
        if (outputRootPathLocal == null) {
            throw new IllegalArgumentException("Writer property [" + CarbonS3Property.DATA_LOCAL_PATH + "] is not set.");
        }
        final String writePartition = System.currentTimeMillis() + "_" + UUID.randomUUID();
        final String writePath = outputRootPathLocal + "_" + writePartition + "/";
        final CarbonTable tableCloned = CarbonTable.buildFromTableInfo(TableInfo.deserialize(table.getTableInfo().serialize()));
        tableCloned.getTableInfo().setTablePath(writePath);
        final Configuration configuration = getS3AccessConfiguration(writerProperties);
        final org.apache.carbondata.sdk.file.CarbonWriter writer;
        try {
            writer = org.apache.carbondata.sdk.file.CarbonWriter.builder()
                .outputPath("")
                .writtenBy("flink")
                .withTable(tableCloned)
                .withTableProperties(getTableProperties(tableProperties))
                .withJsonInput(getTableSchema(tableCloned))
                .withHadoopConf(configuration)
                .withThreadSafe((short)2)
                .build();
        } catch (InvalidLoadOptionException exception) {
            // TODO
            throw new UnsupportedOperationException(exception);
        }
        return new CarbonS3Writer(table, writer, writePath, writePartition, writerProperties, configuration);
    }

    @Override
    protected CarbonTable getTable(final Properties writerProperties) throws IOException {
        setS3AccessConfiguration(getS3AccessConfiguration(writerProperties));
        return super.getTable(writerProperties);
    }

    private static Map<String, String> getTableProperties(final Properties properties) {
        final Map<String, String> tableProperties = new HashMap<>(properties.size());
        for (String propertyName : properties.stringPropertyNames()) {
            tableProperties.put(propertyName, properties.getProperty(propertyName));
        }
        return tableProperties;
    }

    private static Schema getTableSchema(final CarbonTable table) {
        final List<CarbonColumn> columnList = table.getCreateOrderColumn(table.getTableName());
        final List<ColumnSchema> columnSchemaList = new ArrayList<>();
        for (CarbonColumn column : columnList) {
            columnSchemaList.add(column.getColumnSchema());
        }
        return new Schema(columnSchemaList);
    }

    private static Configuration getS3AccessConfiguration(final Properties writerProperties) {
        final String accessKey = writerProperties.getProperty(CarbonS3Property.ACCESS_KEY);
        final String secretKey = writerProperties.getProperty(CarbonS3Property.SECRET_KEY);
        final String endpoint = writerProperties.getProperty(CarbonS3Property.ENDPOINT);
        if (secretKey == null) {
            throw new IllegalArgumentException("Writer property [" + CarbonS3Property.ACCESS_KEY + "] is not set.");
        }
        if (accessKey == null) {
            throw new IllegalArgumentException("Writer property [" + CarbonS3Property.SECRET_KEY + "] is not set.");
        }
        if (endpoint == null) {
            throw new IllegalArgumentException("Writer property [" + CarbonS3Property.ENDPOINT + "] is not set.");
        }

        final Configuration configuration = new Configuration(true);
        configuration.set("fs.s3.access.key", accessKey);
        configuration.set("fs.s3.secret.key", secretKey);
        configuration.set("fs.s3.endpoint", endpoint);
        configuration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("fs.s3a.access.key", accessKey);
        configuration.set("fs.s3a.secret.key", secretKey);
        configuration.set("fs.s3a.endpoint", endpoint);
        configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        configuration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        return configuration;
    }

    private static void setS3AccessConfiguration(final Configuration configuration) {
        ThreadLocalSessionInfo.setConfigurationToCurrentThread(configuration);
    }

}
