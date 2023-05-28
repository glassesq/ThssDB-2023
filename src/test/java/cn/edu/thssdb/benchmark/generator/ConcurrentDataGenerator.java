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

public class ConcurrentDataGenerator extends BaseDataGenerator {

  @Override
  protected void initTableSchema() {
    List<String> columns = new ArrayList<>();
    List<DataType> types = new ArrayList<>();
    List<Boolean> notNull = new ArrayList<>();
    for (int columnId = 0; columnId < Constants.columnTypes.length; columnId++) {
      columns.add("column" + columnId);
      types.add(Constants.columnTypes[columnId % Constants.columnTypes.length]);
      notNull.add(columnId == 0);
    }
    schemaMap.put(
        "concurrent_table_1", new TableSchema("concurrent_table_1", columns, types, notNull, 0));
    schemaMap.put(
        "concurrent_table_2", new TableSchema("concurrent_table_2", columns, types, notNull, 0));
  }

  @Override
  public Object generateValue(String tableName, int rowId, int columnId) {
    if (columnId == 0 || columnId == rowId % 5) {
      switch (schemaMap.get(tableName).types.get(columnId)) {
        case INT:
          return rowId;
        case LONG:
          return (long) rowId;
        case DOUBLE:
          return (double) rowId;
        case FLOAT:
          return (float) rowId;
        case STRING:
          return String.format(stringFormat, rowId);
      }
    }
    return null;
  }
}
