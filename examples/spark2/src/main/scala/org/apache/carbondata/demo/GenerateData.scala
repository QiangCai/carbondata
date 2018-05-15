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


package org.apache.carbondata.demo

import java.io.FileWriter
import java.util.Random

object GenerateData {

  def main(args: Array[String]): Unit = {
    val path = "/home/david/Documents/code/carbondata/examples/spark2/src/main/resources" +
               "/demo_data.csv"
    val fileWriter = new FileWriter(path)
    (0 until 10000).foreach { i =>
      if (i == 0) {
        fileWriter.write(s"$i,${generateString(i)},${ i * 10 },${ i }$$${ i + 1 }$$${ i + 2 }")
      } else {
        fileWriter.write(s"\n$i,${generateString(i)},${ i * 10 },${ i }$$${ i + 1 }$$${ i + 2 }")
      }
    }
    fileWriter.close()
  }

  val chars = Array('a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z')
  val random = new Random()
  def generateString(i: Int): String = {
    new String(Array(
      chars(random.nextInt(26)),
      chars(random.nextInt(26)),
      chars(random.nextInt(26)),
      chars(random.nextInt(26)),
      chars(random.nextInt(26))
    ))
  }
}
