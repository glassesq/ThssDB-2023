package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.ValueWrapper;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.storage.page.IndexPage;
import cn.edu.thssdb.utils.Pair;

public class DeletePlan extends LogicalPlan {
  public boolean broken = false;
  public boolean useWhere;
  public String tableName;
  public SQLParser.ColumnFullNameContext L_where;
  public SQLParser.LiteralValueContext R_where;
  public ValueWrapper queryValue;
  public Column queryCol;
  public SQLParser.ComparatorContext cmp_where;
  public long transactionId = -1;

  public Table.TableMetadata tableMetadata;

  public void initialization(Table.TableMetadata table) {
    this.tableMetadata = table;
    System.out.println("delete initialization start");
    System.out.println("useWhere = " + useWhere);
    if (useWhere) {
      String keyName = L_where.columnName().getText();
      if (table == null)
        throw new IllegalArgumentException(
            "Table " + L_where.tableName().getText() + " not found in FROM clause.");
      if (table.columnNames.get(keyName) == null)
        throw new IllegalArgumentException(
            "Column '" + keyName + "' not found in table '" + table.name + "'");

      queryCol = table.getColumnDetailByName(keyName);
      queryValue = new ValueWrapper(queryCol);
      queryValue.setWithNull(R_where.getText());
    }
  }

  public void deleteLess(Table.TableMetadata table) throws Exception {
    IndexPage rootPage =
        (IndexPage) IO.read(table.spaceId, ServerRuntime.config.indexRootPageIndex);
    IndexPage.recordCondition condition =
        (recordInPage) ->
            checkCondition(
                getRecordInPageValue(recordInPage, queryCol.primary), queryValue, cmp_where);

    Pair<Integer, Integer> pageIter = new Pair<>(null, null);
    pageIter.left = rootPage.deleteFromLeftmostDataPage(transactionId, condition, null);
    IndexPage rightPage;

    ValueWrapper[] query = {queryValue};

    while (pageIter.left > 0) {
      rightPage = (IndexPage) IO.read(table.spaceId, pageIter.left);
      pageIter = rightPage.deleteWithPrimaryCondition(transactionId, condition, query, null);
      if (pageIter.right > 0) break;
    }
  }

  public void deleteGreater(Table.TableMetadata table) throws Exception {
    IndexPage rootPage =
        (IndexPage) IO.read(table.spaceId, ServerRuntime.config.indexRootPageIndex);
    ValueWrapper[] query = {queryValue};
    IndexPage.recordCondition condition =
        (recordInPage) ->
            checkCondition(
                getRecordInPageValue(recordInPage, queryCol.primary), queryValue, cmp_where);

    Pair<Integer, Integer> pageResult =
        rootPage.scanTreeAndDeleteFromPage(transactionId, query, condition, null);
    IndexPage rightPage;

    boolean deleteAll = false;
    while (pageResult.left > 0) {
      if (deleteAll) {
        rightPage = (IndexPage) IO.read(table.spaceId, pageResult.left);
        pageResult.left = rightPage.deleteAll(transactionId, null);
      } else {
        rightPage = (IndexPage) IO.read(table.spaceId, pageResult.left);
        pageResult = rightPage.deleteWithPrimaryCondition(transactionId, condition, query, null);
        if (pageResult.right < 0) deleteAll = true;
      }
    }
  }

  public void deleteEqual(Table.TableMetadata table) throws Exception {
    IndexPage rootPage =
        (IndexPage) IO.read(table.spaceId, ServerRuntime.config.indexRootPageIndex);
    ValueWrapper[] queryKey = {queryValue};
    rootPage.scanTreeAndDeleteRecordWithKey(transactionId, queryKey);
  }

  public boolean checkCondition(ValueWrapper A, ValueWrapper B, SQLParser.ComparatorContext cmp) {
    if (cmp.NE() != null && A.compareTo(B) != 0) return true;
    if (cmp.EQ() != null && A.compareTo(B) == 0) return true;
    if (cmp.LE() != null && A.compareTo(B) <= 0) return true;
    if (cmp.LT() != null && A.compareTo(B) < 0) return true;
    if (cmp.GE() != null && A.compareTo(B) >= 0) return true;
    return cmp.GT() != null && A.compareTo(B) > 0;
  }

  public void deleteCondition(Table.TableMetadata table) throws Exception {
    System.out.println("delete condition");
    IndexPage rootPage =
        (IndexPage) IO.read(table.spaceId, ServerRuntime.config.indexRootPageIndex);
    IndexPage.recordCondition condition;
    if (useWhere) {
      condition =
          (recordInPage) ->
              checkCondition(
                  getRecordInPageValue(recordInPage, queryCol.primary), queryValue, cmp_where);
    } else {
      condition = (recordInPage) -> true;
    }

    int pageIter = rootPage.deleteFromLeftmostDataPage(transactionId, condition, null);
    IndexPage rightpage;
    while (pageIter > 0) {
      rightpage = (IndexPage) IO.read(table.spaceId, pageIter);
      pageIter = rightpage.deleteWithCondition(transactionId, condition, null);
    }
  }

  public ValueWrapper getRecordInPageValue(IndexPage.RecordInPage record, int primary) {
    return primary < 0
        ? record.nonPrimaryKeyValues[-primary - 1]
        : record.primaryKeyValues[primary];
  }

  public void doDelete(long transactionId, Table.TableMetadata table) throws Exception {
    this.transactionId = transactionId;
    initialization(table);
    System.out.println("initialization finished.");
    if (!useWhere) {
      deleteCondition(table);
    } else {
      if (table.getPrimaryKeyNumber() == 1
          && L_where.columnName().getText().equals(table.getPrimaryKeyList().get(0))) {
        if (cmp_where.EQ() != null) {
          deleteEqual(table);
        } else if (cmp_where.LE() != null || cmp_where.LT() != null) {
          deleteLess(table);
        } else if (cmp_where.GE() != null || cmp_where.GT() != null) {
          deleteGreater(table);
        } else if (cmp_where.NE() != null) {
          deleteCondition(table);
        }
      } else {
        deleteCondition(table);
      }
    }
  }

  public void init_where(SQLParser.ConditionContext cond) {
    if (!useWhere) return;
    L_where = cond.expression(0).comparer().columnFullName();
    R_where = cond.expression(1).comparer().literalValue();
    cmp_where = cond.comparator();
  }

  public DeletePlan(
      String tableName, SQLParser.ConditionContext condition_where, boolean useWhere) {
    super(LogicalPlanType.DELETE);
    this.tableName = tableName;
    this.useWhere = useWhere;
    init_where(condition_where);
  }
}
