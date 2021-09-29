/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.v2.file.metadata.statistics;

import org.apache.iotdb.tsfile.exception.write.UnknownColumnTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class StatisticsV2 {

  private StatisticsV2() {}

  @SuppressWarnings("rawtypes")
  public static Statistics deserialize(InputStream inputStream, TSDataType dataType)
      throws IOException {
    Statistics<?> statistics = Statistics.getStatsByType(dataType);
    statistics.setCount((int) ReadWriteIOUtils.readLong(inputStream));
    statistics.setStartTime(ReadWriteIOUtils.readLong(inputStream));
    statistics.setEndTime(ReadWriteIOUtils.readLong(inputStream));
    switch (dataType) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case TEXT:
      case DOUBLE:
      case FLOAT:
        statistics.deserialize(inputStream);
        break;
      default:
        throw new UnknownColumnTypeException(dataType.toString());
    }
    statistics.setEmpty(false);
    return statistics;
  }

  @SuppressWarnings("rawtypes")
  public static Statistics deserialize(ByteBuffer buffer, TSDataType dataType) {
    Statistics<?> statistics = Statistics.getStatsByType(dataType);
    statistics.setCount((int) ReadWriteIOUtils.readLong(buffer));
    statistics.setStartTime(ReadWriteIOUtils.readLong(buffer));
    statistics.setEndTime(ReadWriteIOUtils.readLong(buffer));
    switch (dataType) {
      case BOOLEAN:
      case INT32:
      case INT64:
      case TEXT:
      case DOUBLE:
      case FLOAT:
        statistics.deserialize(buffer);
        break;
      default:
        throw new UnknownColumnTypeException(dataType.toString());
    }
    statistics.setEmpty(false);
    return statistics;
  }
}
