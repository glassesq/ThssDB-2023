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
package cn.edu.thssdb.parser;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.CreateDatabasePlan;
import cn.edu.thssdb.plan.impl.CreateTablePlan;
import cn.edu.thssdb.plan.impl.InsertPlan;
import cn.edu.thssdb.plan.impl.UseDatabasePlan;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.DataType;

import java.util.ArrayList;
import java.util.List;

public class ThssDBSQLVisitor extends SQLBaseVisitor<LogicalPlan> {

    @Override
    public LogicalPlan visitCreateDbStmt(SQLParser.CreateDbStmtContext ctx) {
        return new CreateDatabasePlan(ctx.databaseName().getText());
    }

    @Override
    public LogicalPlan visitUseDbStmt(SQLParser.UseDbStmtContext ctx) {
        return new UseDatabasePlan(ctx.databaseName().getText());
    }

    /**
     * visit create-table statement and prepare a metadata for it.
     * This metadata is not managed by ServerRuntime by now.
     *
     * @param ctx the parse tree
     * @return plan of create-table statement
     */
    @Override
    public LogicalPlan visitCreateTableStmt(SQLParser.CreateTableStmtContext ctx) {
        Table.TableMetadata tableMetadata = new Table.TableMetadata();
        tableMetadata.prepare(ctx.tableName().getText(), ServerRuntime.newTablespace());

        for (SQLParser.ColumnDefContext columnContext : ctx.columnDef()) {
            String name = columnContext.columnName().getText();
            String strType = columnContext.typeName().getText();
            DataType type = Column.str2DataType(strType);
            int length = 0;
            if (type == DataType.STRING) length =
                    Integer.parseInt(strType.substring(7, strType.length() - 1)) * ServerRuntime.config.maxCharsetLength;
            /* additional length are required for charset */

            Column column = new Column();
            column.prepare(name, type, length);
            for (SQLParser.ColumnConstraintContext constraintContext : columnContext.columnConstraint()) {
                column.setConstraint(constraintContext.getText());
            }
            tableMetadata.addColumn(column);
        }

        int order = 0;
        for (SQLParser.ColumnNameContext columnNameContext : ctx.tableConstraint().columnName()) {
            tableMetadata.columnDetails.get(tableMetadata.columns.get(columnNameContext.getText())).setPrimaryKey(order);
            ++order;
        }

        return new CreateTablePlan(tableMetadata);
    }

    public LogicalPlan visitInsertStmt(SQLParser.InsertStmtContext ctx) {
        String tableName = ctx.tableName().getText();

        List<SQLParser.ColumnNameContext> columnNames = ctx.columnName();
        ArrayList<String> columnName = new ArrayList<>();
        for (SQLParser.ColumnNameContext column : columnNames) {
            columnName.add(column.getText());
        }

        List<SQLParser.ValueEntryContext> valueEntries = ctx.valueEntry();
        ArrayList<ArrayList<String>> values = new ArrayList<>();
        for (SQLParser.ValueEntryContext valueEntry : valueEntries) {
            if (columnName.size() > 0 && valueEntry.literalValue().size() != columnName.size()) {
                return new InsertPlan(tableName, true);
            }
            ArrayList<String> value = new ArrayList<>();
            for (SQLParser.LiteralValueContext literalValueContext : valueEntry.literalValue()) {
                value.add(literalValueContext.getText());
            }
            values.add(value);
        }

        return new InsertPlan(tableName, columnName, values);
    }

    // TODO: parser to more logical plan
}
