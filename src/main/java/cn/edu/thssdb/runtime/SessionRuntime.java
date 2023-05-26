package cn.edu.thssdb.runtime;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.StatusUtil;

import java.util.ArrayList;

/** The runtime of one session. */
public class SessionRuntime {

  /** current database under the session's use. */
  public int databaseId = -1;

  /** current 8-byte transaction id under the session's use */
  public long transactionId = -1;

  /** stop the session. */
  public void stop() {
    // TODO
  }

  /**
   * run plan inter the session. create transaction if necessary.
   *
   * @param plan the plan to be executed.
   * @return executeStatementResponse
   */
  public ExecuteStatementResp runPlan(LogicalPlan plan) {
    try {

      /* Transaction Free Statement. */
      switch (plan.getType()) {
        case USE_DATABASE:
          // TODO: require LOCK
          UseDatabasePlan useDatabasePlan = (UseDatabasePlan) plan;
          if (ServerRuntime.databaseNameLookup.containsKey(useDatabasePlan.getDatabaseName())) {
            databaseId = ServerRuntime.databaseNameLookup.get(useDatabasePlan.getDatabaseName());
            return new ExecuteStatementResp(
                StatusUtil.success("switch to database " + useDatabasePlan.getDatabaseName()),
                false);
          } else {
            return new ExecuteStatementResp(
                StatusUtil.fail("cannot find database " + useDatabasePlan.getDatabaseName()),
                false);
          }
        default:
      }

      /* Transaction Needed Statement. */
      if (transactionId < 0 && ServerRuntime.config.allow_implicit_transaction) {
        // automatically begin the transaction if allow_implicit_transaction is on.
        transactionId = ServerRuntime.newTransaction();
        IO.writeTransactionStart(transactionId);
      } else if (transactionId < 0) {
        return new ExecuteStatementResp(
            StatusUtil.fail(
                "There is no active transaction now. Please begin a transaction first."),
            false);
      }
      ExecuteStatementResp response = null;
      System.out.println(plan); // For Test
      switch (plan.getType()) {
        case COMMIT:
          IO.pushTransactionCommit(transactionId);
          transactionId = -1;
          // Commit statement shall be treated as the end of transaction. No matter it succeeds or
          // not.
          // TODO: If commit failed, the transaction shall enter its abort process.
          // see also at the end of the function

          // release all locks
          ServerRuntime.releaseAllLocks(transactionId);
          return new ExecuteStatementResp(
              StatusUtil.success("The transaction has been successfully committed."), false);
        case CREATE_DATABASE:
          CreateDatabasePlan createDatabasePlan = (CreateDatabasePlan) plan;
          String name = createDatabasePlan.getDatabaseName();
          if (Database.DatabaseMetadata.createDatabase(
                  transactionId, createDatabasePlan.getDatabaseName())
              == null)
            response =
                new ExecuteStatementResp(
                    StatusUtil.fail("Database " + name + " already existed."), false);
          else
            response =
                new ExecuteStatementResp(
                    StatusUtil.success("Database " + name + " created."), false);
          break;
        default:
      }

      if (response != null) {
        if (ServerRuntime.config.auto_commit) {
          IO.pushTransactionCommit(transactionId);
          transactionId = -1;
          response.status.msg =
              response.status.msg + "\n\nEnd of the transaction.(auto commit on).";
        }
        return response;
      }

      Database.DatabaseMetadata currentDatabaseMetadata =
          ServerRuntime.databaseMetadata.get(databaseId);
      if (currentDatabaseMetadata == null) {
        return new ExecuteStatementResp(
            StatusUtil.fail("There is no active database now. Please use a database first."),
            false);
      }

      switch (plan.getType()) {
        case CREATE_TABLE:
          CreateTablePlan createTablePlan = (CreateTablePlan) plan;
          if (createTablePlan.broken)
            return new ExecuteStatementResp(StatusUtil.fail("The statement is broken."), false);
          String name = createTablePlan.tableMetadata.name;
          if (currentDatabaseMetadata.getTableByName(name) != null)
            return new ExecuteStatementResp(StatusUtil.fail("Table " + name + " existed."), false);
          currentDatabaseMetadata.createTable(transactionId, createTablePlan.tableMetadata);
          response =
              new ExecuteStatementResp(StatusUtil.success("Table " + name + " created."), false);
          break;
        case SHOW_TABLE:
          ShowTablePlan showTablePlan = (ShowTablePlan) plan;
          Table.TableMetadata showTable =
              currentDatabaseMetadata.getTableByName(showTablePlan.tableName);
          if (showTable == null)
            return new ExecuteStatementResp(
                StatusUtil.fail("Table " + showTablePlan.tableName + " not found."), false);
          showTablePlan.getValue(showTable);
          response = new ExecuteStatementResp(StatusUtil.success(showTablePlan.toString()), false);
          break;
        case INSERT:
          InsertPlan insertPlan = (InsertPlan) plan;
          if (insertPlan.broken)
            return new ExecuteStatementResp(StatusUtil.fail("The statement is broken."), false);
          Table.TableMetadata table = currentDatabaseMetadata.getTableByName(insertPlan.tableName);
          if (table == null)
            return new ExecuteStatementResp(
                StatusUtil.fail("Table " + insertPlan.tableName + " not found."), false);
          ArrayList<ArrayList<String>> results = insertPlan.getValues(table);
          for (ArrayList<String> result : results) {
            table.insertRecord(transactionId, result);
          }
          response = new ExecuteStatementResp(StatusUtil.success("Insertion succeeded."), false);
          break;
        case SELECT:
          SelectPlan selectPlan = (SelectPlan) plan;
          if (selectPlan.broken)
            return new ExecuteStatementResp(StatusUtil.fail("The statement is broken."), false);
          ArrayList<Table.TableMetadata> tables = new ArrayList<>();
          for (String tableName : selectPlan.tableNames) {
            System.out.println(tableName);
            System.out.println(currentDatabaseMetadata.getTableByName(tableName));
            tables.add(currentDatabaseMetadata.getTableByName(tableName));
          }
          System.out.println("SELECT starting");
          QueryResult result = selectPlan.getResult(transactionId, tables);
          System.out.println("SELECT finished");
          response =
              new ExecuteStatementResp(StatusUtil.success("Select operation completed"), true);
          response.setColumnsList(result.columns);
          for (ArrayList<String> row : result.rows) {
            response.addToRowList(row);
            int i = 0;
            for (String e : row) {
              System.out.println(result.columns.get(i) + e);
              ++i;
            }
          }
        default:
      }

      if (response != null) {
        if (ServerRuntime.config.auto_commit) {
          IO.pushTransactionCommit(transactionId);
          // This commit is only for test.
          // TODO: Suitable validation should be introduced.
          // If only NOT NULL as well as PRIMARY KEY constraints are implemented, we can sacrifice
          // functionality for performance.
          transactionId = -1;
          response.status.msg =
              response.status.msg + "\n\nEnd of the transaction.(auto commit on).";
        }
        return response;
      }
      return new ExecuteStatementResp(
          StatusUtil.fail("Command not understood or implemented."), false);
    } catch (Exception e) {
      return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
    }
  }
}
