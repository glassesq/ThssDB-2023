package cn.edu.thssdb.runtime;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.storage.writeahead.DummyLog;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;

import java.util.ArrayList;

/** The runtime of one session. */
public class SessionRuntime {

  public long sessionId = -1;

  /** current database under the session's use. */
  public int databaseId = -1;

  /** current 8-byte transaction id under the session's use */
  public long transactionId = -1;

  public boolean usingBeginTransaction = false;

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
  public ExecuteStatementResp runPlan(LogicalPlan plan, String statement) {
    switch (plan.getType()) {
      case BEGIN_TRANSACTION:
        usingBeginTransaction = true;
        if (transactionId < 0) {
          transactionId = ServerRuntime.newTransaction();
          IO.writeTransactionStart(sessionId, transactionId);
          return new ExecuteStatementResp(
              StatusUtil.success("You are now in transaction " + transactionId), false);
        } else {
          IO.pushTransactionCommit(transactionId);
          ServerRuntime.releaseAllLocks(transactionId);
          transactionId = -1;
          transactionId = ServerRuntime.newTransaction();
          IO.writeTransactionStart(sessionId, transactionId);
          return new ExecuteStatementResp(
              StatusUtil.success("There is already an active transaction now."), false);
        }
      case USE_DATABASE:
        UseDatabasePlan useDatabasePlan = (UseDatabasePlan) plan;
        if (ServerRuntime.databaseNameLookup.containsKey(useDatabasePlan.getDatabaseName())) {
          databaseId = ServerRuntime.databaseNameLookup.get(useDatabasePlan.getDatabaseName());
          DummyLog.writeDummyLog(transactionId, sessionId + ":success:" + statement);
          return new ExecuteStatementResp(
              StatusUtil.success("switch to database " + useDatabasePlan.getDatabaseName()), false);
        } else {
          return new ExecuteStatementResp(
              StatusUtil.fail("cannot find database " + useDatabasePlan.getDatabaseName()), false);
        }
      default:
    }

    /* Transaction Needed Statement. */
    if (transactionId < 0 && ServerRuntime.config.allow_implicit_transaction) {
      // automatically begin the transaction if allow_implicit_transaction is on.
      usingBeginTransaction = false;
      transactionId = ServerRuntime.newTransaction();
      IO.writeTransactionStart(sessionId, transactionId);
    } else if (transactionId < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("There is no active transaction now. Please begin a transaction first."),
          false);
    }
    ExecuteStatementResp response = null;
    switch (plan.getType()) {
      case COMMIT:
        IO.pushTransactionCommit(transactionId);
        ServerRuntime.releaseAllLocks(transactionId);
        transactionId = -1;
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
              new ExecuteStatementResp(StatusUtil.success("Database " + name + " created."), false);
        break;
      case DROP_DATABASE:
        DropDatabasePlan dropDatabasePlan = (DropDatabasePlan) plan;
        String dropDbName = dropDatabasePlan.getDatabaseName();
        if (!Database.DatabaseMetadata.dropDatabase(transactionId, dropDbName))
          response =
              new ExecuteStatementResp(
                  StatusUtil.fail("Database " + dropDbName + " not exists."), false);
        else
          response =
              new ExecuteStatementResp(
                  StatusUtil.success("Database " + dropDbName + " dropped."), false);
        break;
      default:
    }

    if (response != null) {
      if (response.status.getCode() == Global.SUCCESS_CODE && !response.hasResult)
        IO.writeDummyStatementLog(transactionId, sessionId + ":success:" + statement);
      if (!usingBeginTransaction && ServerRuntime.config.auto_commit) {
        IO.pushTransactionCommit(transactionId);
        ServerRuntime.releaseAllLocks(transactionId);
        transactionId = -1;
        response.status.msg = response.status.msg + "\n\nEnd of the transaction.(auto commit on).";
      }
      return response;
    }

    // TODO: wrap databaseMetadata.get and add metadataLatch to protect it.
    Database.DatabaseMetadata currentDatabaseMetadata =
        ServerRuntime.databaseMetadata.get(databaseId);
    if (currentDatabaseMetadata == null) {
      // TODO: roll back
      IO.pushTransactionCommit(transactionId);
      ServerRuntime.releaseAllLocks(transactionId);
      transactionId = -1;
      return new ExecuteStatementResp(
          StatusUtil.fail("There is no active database now. Please use a database first."), false);
    }

    switch (plan.getType()) {
      case CREATE_TABLE:
        CreateTablePlan createTablePlan = (CreateTablePlan) plan;
        if (createTablePlan.broken) {
          response = new ExecuteStatementResp(StatusUtil.fail("The statement is broken."), false);
          break;
        }
        String name = createTablePlan.tableMetadata.name;
        if (currentDatabaseMetadata.getTableByName(name) != null) {
          response =
              new ExecuteStatementResp(StatusUtil.fail("Table " + name + " existed."), false);
          break;
        }
        currentDatabaseMetadata.createTable(transactionId, createTablePlan.tableMetadata);
        response =
            new ExecuteStatementResp(StatusUtil.success("Table " + name + " created."), false);
        break;
      case SHOW_TABLE:
        ShowTablePlan showTablePlan = (ShowTablePlan) plan;
        Table.TableMetadata showTable =
            currentDatabaseMetadata.getTableByName(showTablePlan.tableName);
        if (showTable == null) {
          response =
              new ExecuteStatementResp(
                  StatusUtil.fail("Table " + showTablePlan.tableName + " not found."), false);
          break;
        }
        showTablePlan.getValue(showTable);
        response = new ExecuteStatementResp(StatusUtil.success(showTablePlan.toString()), false);
        break;
      case INSERT:
        InsertPlan insertPlan = (InsertPlan) plan;
        if (insertPlan.broken) {
          response = new ExecuteStatementResp(StatusUtil.fail("The statement is broken."), false);
          break;
        }
        Table.TableMetadata table = currentDatabaseMetadata.getTableByName(insertPlan.tableName);
        if (table == null) {
          response =
              new ExecuteStatementResp(
                  StatusUtil.fail("Table " + insertPlan.tableName + " not found."), false);
          break;
        }
        ArrayList<ArrayList<String>> results;
        try {
          results = insertPlan.getValues(table);
        } catch (Exception e) {
          e.printStackTrace();
          response =
              new ExecuteStatementResp(
                  StatusUtil.fail("Table " + insertPlan.tableName + " not found."), false);
          break;
        }
        boolean insertResult = true;
        int index;
        for (index = 0; index < results.size(); index++) {
          insertResult = table.insertRecord(transactionId, results.get(index));
          if (!insertResult) break;
        }
        if (insertResult) {
          response = new ExecuteStatementResp(StatusUtil.success("Insertion succeeded."), false);
        } else {
          response =
              new ExecuteStatementResp(StatusUtil.fail("The primary key already exists."), false);
        }
        break;
      case SELECT:
        SelectPlan selectPlan = (SelectPlan) plan;
        if (selectPlan.broken) {
          response = new ExecuteStatementResp(StatusUtil.fail("The statement is broken."), false);
          break;
        }
        ArrayList<Table.TableMetadata> tables = new ArrayList<>();
        for (String tableName : selectPlan.tableNames) {
          tables.add(currentDatabaseMetadata.getTableByName(tableName));
        }
        QueryResult result;
        try {
          result = selectPlan.getResult(transactionId, tables);
        } catch (Exception e) {
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
          break;
        }

        response = new ExecuteStatementResp(StatusUtil.success("Select operation completed"), true);
        response.setColumnsList(result.columns);
        for (ArrayList<String> row : result.rows) {
          response.addToRowList(row);
        }
        break;
      case DELETE:
        DeletePlan deletePlan = (DeletePlan) plan;
        if (deletePlan.broken) {
          response = new ExecuteStatementResp(StatusUtil.fail("The statement is broken."), false);
          break;
        }
        Table.TableMetadata metadata = currentDatabaseMetadata.getTableByName(deletePlan.tableName);
        if (metadata == null) {
          response =
              new ExecuteStatementResp(
                  StatusUtil.fail("Table " + deletePlan.tableName + " not found."), false);
          break;
        }
        try {
          deletePlan.doDelete(transactionId, metadata);
        } catch (Exception e) {
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
          break;
        }
        response =
            new ExecuteStatementResp(StatusUtil.success("delete operation completed"), false);
        break;
      case UPDATE:
        UpdatePlan updatePlan = (UpdatePlan) plan;
        if (updatePlan.broken) {
          response = new ExecuteStatementResp(StatusUtil.fail("The statement is broken."), false);
          break;
        }
        Table.TableMetadata updateMetadata =
            currentDatabaseMetadata.getTableByName(updatePlan.tableName);
        if (updateMetadata == null) {
          response =
              new ExecuteStatementResp(
                  StatusUtil.fail("Table " + updatePlan.tableName + " not found."), false);
          break;
        }
        boolean updateResult;
        try {
          updateResult = updatePlan.doUpdate(transactionId, updateMetadata);
        } catch (Exception e) {
          response = new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
          break;
        }
        response =
            new ExecuteStatementResp(StatusUtil.success("update operation completed"), false);
        if (!updateResult)
          response.status.msg =
              "Update Rejected! No changes occur because of the constraint violation.\n"
                  + response.status.msg;
        break;
      default:
    }

    if (response != null) {
      if (response.status.getCode() == Global.SUCCESS_CODE && !response.hasResult)
        IO.writeDummyStatementLog(transactionId, sessionId + ":success:" + statement);
      if (!usingBeginTransaction && ServerRuntime.config.auto_commit) {
        if (response.status.getCode() == Global.FAILURE_CODE) {
          response.status.msg =
              " [WARNING!] The operation not succeed. But we 'commit' it for now.\n"
                  + response.status.msg;
        }
        IO.pushTransactionCommit(transactionId);
        ServerRuntime.releaseAllLocks(transactionId);
        transactionId = -1;
        response.status.msg = response.status.msg + "\n\nEnd of the transaction.(auto commit on).";
      }
      return response;
    }
    return new ExecuteStatementResp(
        StatusUtil.fail("Command not understood or implemented."), false);
  }
  /*    } catch (Exception e) {

  ExecuteStatementResp response =
      new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
  if (!usingBeginTransaction && ServerRuntime.config.auto_commit) {
    response.status.msg =
        " [WARNING!] The operation not succeed. But we 'commit' it for now.\n"
            + response.status.msg;
    try {
      IO.pushTransactionCommit(transactionId);
    } catch (Exception shallNeverHappen) {
       We shall shut down the database, restart and recover it.
      System.out.println(shallNeverHappen.getMessage());
      exit(3);
    }
    ServerRuntime.releaseAllLocks(transactionId);
    transactionId = -1;
    response.status.msg = response.status.msg + "\n\nEnd of the transaction.(auto commit on).";
  } */

}
