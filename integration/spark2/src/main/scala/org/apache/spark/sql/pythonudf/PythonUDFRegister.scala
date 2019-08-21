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

package org.apache.spark.sql.pythonudf

import java.io.{File, FileWriter, IOException}

import org.apache.spark.api.python.{PythonBroadcast, PythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.carbondata.core.datastore.impl.FileFactory

object PythonUDFRegister {

  def registerPythonUDF(spark: SparkSession,
      udfName: String,
      funcName: String,
      script: String,
      libraryIncludes: Array[String],
      returnType: DataType = StringType,
      usePython3AsDefault: Boolean = true): Unit = {
    // Generate a wrapper script to wrap the user input script
    // Run the script to get the serialized python executable object (binary)
    // Register the executable object to spark

    val fileName = generateScriptFile(funcName, script, returnType)

    try {
      val inBinary = PythonExecUtil.runPythonScript(spark, libraryIncludes, fileName, Seq.empty, usePython3AsDefault)
      FileFactory.deleteFile(fileName, FileFactory.getFileType(fileName))

      val default_python_version = if (usePython3AsDefault) "3.6" else "2.7"
      val default_python_exec = if (usePython3AsDefault) "python3" else "python2"

      // TODO handle big udf bigger than 1 MB, they supposed to be broadcasted.
      val function = PythonFunction(
        inBinary,
        new java.util.HashMap[String, String](),
        new java.util.ArrayList[String](),
        spark.sparkContext.getConf.get("spark.python.exec", default_python_exec),
        spark.sparkContext.getConf.get("spark.python.version", default_python_version),
        new java.util.ArrayList[Broadcast[PythonBroadcast]](),
        null)
      spark.udf.registerPython(
        udfName,
        UserDefinedPythonFunction(udfName, function, returnType, 100, true))
    } catch {
      case ex: IOException if ex.getMessage.contains("Cannot run program") =>
        val ignore = spark.sparkContext.getConf.get("spark.pythonUDF.ignoreIfPythonNotFound", "false")
          .equalsIgnoreCase("true")
        if (!ignore) {
          throw ex
        }
    }
  }

  def unregisterPythonUDF(
      spark: SparkSession,
      udfName: String): Unit = {
    spark.sessionState.functionRegistry.dropFunction(FunctionIdentifier(udfName))
  }

  private def generateScriptFile(funcName: String, script: String, returnType: DataType): String = {
    // scalastyle:off
    val gen =
      s"""
         |import os
         |import sys
         |from pyspark.serializers import CloudPickleSerializer
         |from pyspark.sql.types import BooleanType,ByteType,ShortType,IntegerType,LongType,FloatType,DoubleType,DecimalType,StringType,BinaryType,StructType,MapType,ArrayType
         |
         |${ script }
         |ser = CloudPickleSerializer()
         |pickled_command = bytearray(ser.dumps((${ funcName },
         |      ${ returnType.getClass.getSimpleName.replace("$", "") }())))
         |pickled_command
         |stdout_bin = os.fdopen(sys.stdout.fileno(), 'wb', 4)
         |stdout_bin.write(pickled_command)
     """.stripMargin
    // scalastyle:on
    val file = new File(System.getProperty("java.io.tmpdir") + "/python/" +
                        System.nanoTime() + ".py")
    file.getParentFile.mkdirs()
    val writer = new FileWriter(file)
    writer.write(gen)
    writer.close()
    file.getAbsolutePath
  }

}