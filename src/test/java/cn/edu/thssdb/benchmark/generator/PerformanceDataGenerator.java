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

package cn.edu.thssdb.benchmark.generator;

import cn.edu.thssdb.benchmark.common.Constants;
import cn.edu.thssdb.benchmark.common.DataType;
import cn.edu.thssdb.benchmark.common.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PerformanceDataGenerator extends BaseDataGenerator {
  private String stringFormat = "%0" + Constants.stringLength + "d";
  private Random random = new Random(666);

  @Override
  protected void initTableSchema() {
    for (int tableId = 0; tableId < Constants.tableCount; tableId++) {
      String tableName = "test_table" + tableId;
      List<String> columns = new ArrayList<>();
      List<DataType> types = new ArrayList<>();
      List<Boolean> notNull = new ArrayList<>();
      for (int columnId = 0; columnId < Constants.columnCount; columnId++) {
        columns.add("column" + columnId);
        types.add(Constants.columnTypes[columnId % Constants.columnTypes.length]);
        notNull.add(false);
      }
      schemaMap.put(tableName, new TableSchema(tableName, columns, types, notNull, tableId));
    }
  }

  @Override
  public Object generateValue(String tableName, int rowId, int columnId) {
    switch (schemaMap.get(tableName).types.get(columnId)) {
      case INT:
        return Math.abs(random.nextInt());
      case LONG:
        return Math.abs(random.nextLong());
      case DOUBLE:
        return Math.abs(random.nextDouble());
      case FLOAT:
        return Math.abs(random.nextFloat());
      case STRING:
        return String.format(stringFormat, rowId);
    }
    return null;
  }
}
