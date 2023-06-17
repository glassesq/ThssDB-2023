package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.RecordLogical;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.ValueWrapper;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.storage.page.IndexPage;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;

public class UpdatePlan extends LogicalPlan {
  public boolean broken = false;
  public boolean useWhere;

  public boolean conflictingUpdate;

  public boolean updateSingleAndOnlyPrimary;

  public String tableName;

  public Column columnToSet;
  public String columnNameToSet;
  public String valueLiteralToSet;
  public ValueWrapper valueToSet;

  public Column L_queryCol, R_queryCol;
  public SQLParser.ColumnFullNameContext L_where;
  public SQLParser.LiteralValueContext R_where;
  public ValueWrapper queryValue;
  public Column queryCol;
  public SQLParser.ComparatorContext cmp_where;
  public long transactionId = -1;

  public Table.TableMetadata tableMetadata;

  public void initialization(Table.TableMetadata table) {
    //    System.out.println("update initialization start");
    this.tableMetadata = table;
    //    System.out.println("useWhere = " + useWhere);
    if (useWhere) {
      String keyName = L_where.columnName().getText().toLowerCase();
      if (table == null)
        throw new IllegalArgumentException(
            "Table " + L_where.tableName().getText() + " not found.");
      if (table.getColumnDetailByName(keyName) == null)
        throw new IllegalArgumentException(
            "Column '" + keyName + "' not found in table '" + table.name + "'");

      queryCol = table.getColumnDetailByName(keyName);
      queryValue = new ValueWrapper(queryCol);
      queryValue.setWithNull(R_where.getText());
    }
    columnToSet = table.getColumnDetailByName(columnNameToSet);
    valueToSet = new ValueWrapper(columnToSet);
    valueToSet.setWithNull(valueLiteralToSet);
  }

  public boolean updateWherePrimaryLess() throws Exception {
    IndexPage rootPage =
        (IndexPage) IO.read(tableMetadata.spaceId, ServerRuntime.config.indexRootPageIndex);
    IndexPage.recordCondition condition =
        (recordInPage) ->
            checkCondition(
                getRecordInPageValue(recordInPage, queryCol.primary), queryValue, cmp_where);

    ArrayList<IndexPage.RecordInPage> recordsNeedToUpdate = new ArrayList<>();
    Pair<Integer, Integer> pageIter = new Pair<>(null, null);
    pageIter.left =
        rootPage.deleteFromLeftmostDataPage(transactionId, condition, recordsNeedToUpdate);
    IndexPage rightPage;

    if (updateSingleAndOnlyPrimary && recordsNeedToUpdate.size() > 1) {
      for (IndexPage.RecordInPage record : recordsNeedToUpdate) {
        /* recover delete */
        record.unsetDeleted();
        record.write(transactionId, rootPage, record.myOffset);
      }
      return false;
    }
    int maxSize = recordsNeedToUpdate.size();

    ValueWrapper[] query = {queryValue};

    while (pageIter.left > 0) {
      rightPage = (IndexPage) IO.read(tableMetadata.spaceId, pageIter.left);
      pageIter =
          rightPage.deleteWithPrimaryCondition(
              transactionId, condition, query, recordsNeedToUpdate);
      if (pageIter.right > 0) break;

      if (updateSingleAndOnlyPrimary && recordsNeedToUpdate.size() > 1) {
        for (int i = 0; i < maxSize; i++) {
          rootPage.scanTreeAndDeleteRecordWithKey(
              transactionId, recordsNeedToUpdate.get(i).primaryKeyValues);
        }
        for (int i = maxSize; i < recordsNeedToUpdate.size(); i++) {
          /* recover delete */
          IndexPage.RecordInPage record = recordsNeedToUpdate.get(i);
          record.unsetDeleted();
          record.write(transactionId, rightPage, record.myOffset);
        }
        return false;
      } else {
        maxSize = recordsNeedToUpdate.size();
      }
    }

    return updateRecordList(recordsNeedToUpdate);
  }

  public boolean updateWherePrimaryGreater() throws Exception {
    IndexPage rootPage =
        (IndexPage) IO.read(tableMetadata.spaceId, ServerRuntime.config.indexRootPageIndex);
    ValueWrapper[] query = {queryValue};
    IndexPage.recordCondition condition =
        (recordInPage) ->
            checkCondition(
                getRecordInPageValue(recordInPage, queryCol.primary), queryValue, cmp_where);

    ArrayList<IndexPage.RecordInPage> recordsNeedToUpdate = new ArrayList<>();

    Pair<Integer, Integer> pageResult =
        rootPage.scanTreeAndDeleteFromPage(transactionId, query, condition, recordsNeedToUpdate);
    IndexPage rightPage;

    if (updateSingleAndOnlyPrimary && recordsNeedToUpdate.size() > 1) {
      for (IndexPage.RecordInPage record : recordsNeedToUpdate) {
        /* recover delete */
        record.unsetDeleted();
        record.write(transactionId, rootPage, record.myOffset);
      }
      return false;
    }
    int maxSize = recordsNeedToUpdate.size();

    boolean deleteAll = false;
    while (pageResult.left > 0) {
      if (deleteAll) {
        rightPage = (IndexPage) IO.read(tableMetadata.spaceId, pageResult.left);
        pageResult.left = rightPage.deleteAll(transactionId, recordsNeedToUpdate);
      } else {
        rightPage = (IndexPage) IO.read(tableMetadata.spaceId, pageResult.left);
        pageResult =
            rightPage.deleteWithPrimaryCondition(
                transactionId, condition, query, recordsNeedToUpdate);
        if (pageResult.right < 0) deleteAll = true;
      }
      if (updateSingleAndOnlyPrimary && recordsNeedToUpdate.size() > 1) {
        for (int i = 0; i < maxSize; i++) {
          rootPage.scanTreeAndDeleteRecordWithKey(
              transactionId, recordsNeedToUpdate.get(i).primaryKeyValues);
        }
        for (int i = maxSize; i < recordsNeedToUpdate.size(); i++) {
          /* recover delete */
          IndexPage.RecordInPage record = recordsNeedToUpdate.get(i);
          record.unsetDeleted();
          record.write(transactionId, rightPage, record.myOffset);
        }
        return false;
      } else {
        maxSize = recordsNeedToUpdate.size();
      }
    }

    return updateRecordList(recordsNeedToUpdate);
  }

  public boolean updateWherePrimaryEqual() throws Exception {
    IndexPage rootPage =
        (IndexPage) IO.read(tableMetadata.spaceId, ServerRuntime.config.indexRootPageIndex);
    //    System.out.println("update where primary equal!");
    if (updateSingleAndOnlyPrimary) {
      ValueWrapper[] queryKey = {queryValue};
      Integer result = queryValue.compareTo(valueToSet);
      if (result != null && result == 0) {
        /* update table set primaryKey = value where primaryKey = value */
        /* no work shall be done. */
        return true;
      }
      RecordLogical recordDeleted =
          rootPage.scanTreeAndDeleteRecordWithKey(transactionId, queryKey);
      if (recordDeleted == null) {
        /* no such record */
        return true;
      }
      RecordLogical recordToInsert = new RecordLogical(recordDeleted);
      recordToInsert.primaryKeyValues[0].setWithNull(valueLiteralToSet);
      boolean insertResult = rootPage.insertDataRecordIntoTree(transactionId, recordToInsert);
      if (!insertResult) {
        rootPage.insertDataRecordIntoTree(transactionId, recordDeleted);
      } else {
        return true;
      }
    } else {
      //      System.out.println("here we delete, no conflict!" + transactionId);
      ValueWrapper[] queryKey = {queryValue};
      //      System.out.println("try delete." + transactionId);
      RecordLogical recordDeleted =
          rootPage.scanTreeAndDeleteRecordWithKey(transactionId, queryKey);
      //      System.out.println("delete ok." + transactionId);
      if (recordDeleted == null) {
        /* no such record */
        return true;
      }
      RecordLogical recordToInsert = new RecordLogical(recordDeleted);
      if (columnToSet.primary >= 0) {
        recordToInsert.primaryKeyValues[columnToSet.primary].setWithNull(valueLiteralToSet);
      } else {
        recordToInsert.nonPrimaryKeyValues[-columnToSet.primary - 1].setWithNull(valueLiteralToSet);
      }
      //      System.out.println("try insert." + transactionId);
      boolean insertResult = rootPage.insertDataRecordIntoTree(transactionId, recordToInsert);
      //      System.out.println("insert ok." + transactionId);
      if (!insertResult) {
        //        System.out.println("let us recover" + transactionId);
        rootPage.insertDataRecordIntoTree(transactionId, recordDeleted);
      } else {
        return true;
      }
    }
    return true;
  }

  public boolean checkCondition(ValueWrapper A, ValueWrapper B, SQLParser.ComparatorContext cmp) {
    Integer result = A.compareTo(B);
    if (result == null) return false;
    if (cmp.NE() != null && result.intValue() != 0) return true;
    if (cmp.EQ() != null && result.intValue() == 0) return true;
    if (cmp.LE() != null && result.intValue() <= 0) return true;
    if (cmp.LT() != null && result.intValue() < 0) return true;
    if (cmp.GE() != null && result.intValue() >= 0) return true;
    return cmp.GT() != null && result.intValue() > 0;
  }

  public boolean updateRecordList(ArrayList<IndexPage.RecordInPage> recordsToUpdate)
      throws Exception {
    IndexPage rootPage =
        (IndexPage) IO.read(tableMetadata.spaceId, ServerRuntime.config.indexRootPageIndex);

    //    System.out.println("update record list start");
    if (conflictingUpdate) {
      /* shadow insert */
      //      System.out.println("shadow insert start.");
      ArrayList<RecordLogical> shadows = new ArrayList<>();
      boolean noConflict = true;
      for (IndexPage.RecordInPage recordInPage : recordsToUpdate) {
        RecordLogical recordToInsert = new RecordLogical(recordInPage);
        if (columnToSet.primary >= 0) {
          recordToInsert.primaryKeyValues[columnToSet.primary].setWithNull(valueLiteralToSet);
        } else {
          recordToInsert.nonPrimaryKeyValues[-columnToSet.primary - 1].setWithNull(
              valueLiteralToSet);
        }
        //        System.out.println("try to insert: ");
        System.out.println(recordToInsert);
        noConflict = rootPage.insertDataRecordIntoTree(transactionId, recordToInsert);
        //        System.out.println("noConflict: " + noConflict);
        if (!noConflict) break;
        shadows.add(recordToInsert);
      }
      if (!noConflict) {
        /* remove shadow and recover deleted records. */
        for (RecordLogical recordInPage : shadows) {
          rootPage.scanTreeAndDeleteRecordWithKey(transactionId, recordInPage.primaryKeyValues);
        }
        for (IndexPage.RecordInPage recordInPage : recordsToUpdate) {
          rootPage.insertDataRecordIntoTree(transactionId, new RecordLogical(recordInPage));
        }
        return false;
      } else {
        return true;
      }
    } else {
      /* insert */
      //      System.out.println("insert start.");
      for (IndexPage.RecordInPage recordInPage : recordsToUpdate) {
        RecordLogical recordToInsert = new RecordLogical(recordInPage);
        if (columnToSet.primary >= 0) {
          recordToInsert.primaryKeyValues[columnToSet.primary].setWithNull(valueLiteralToSet);
        } else {
          recordToInsert.nonPrimaryKeyValues[-columnToSet.primary - 1].setWithNull(
              valueLiteralToSet);
        }
        //        System.out.println("new record to insert:");
        //        System.out.println(recordInPage);
        rootPage.insertDataRecordIntoTree(transactionId, recordToInsert);
      }
    }
    return true;
  }

  public boolean updateCondition() throws Exception {
    //    System.out.println("getCondition");

    IndexPage.recordCondition condition;
    if (useWhere) {
      condition =
          (recordInPage) ->
              checkCondition(
                  getRecordInPageValue(recordInPage, queryCol.primary), queryValue, cmp_where);
    } else {
      condition = (recordInPage) -> true;
    }

    IndexPage rootPage =
        (IndexPage) IO.read(tableMetadata.spaceId, ServerRuntime.config.indexRootPageIndex);
    ArrayList<IndexPage.RecordInPage> recordsNeedToUpdate = new ArrayList<>();

    int nextPageId =
        rootPage.deleteFromLeftmostDataPage(transactionId, condition, recordsNeedToUpdate);

    //    System.out.println("delete these:");

    //    for (IndexPage.RecordInPage record : recordsNeedToUpdate) {
    //      System.out.println(record);
    //    }

    if (updateSingleAndOnlyPrimary && recordsNeedToUpdate.size() > 1) {
      for (IndexPage.RecordInPage record : recordsNeedToUpdate) {
        /* recover delete */
        record.unsetDeleted();
        record.write(transactionId, rootPage, record.myOffset);
      }
      return false;
    }
    int maxSize = recordsNeedToUpdate.size();

    IndexPage rightpage;
    while (nextPageId > 0) {

      rightpage = (IndexPage) IO.read(tableMetadata.spaceId, nextPageId);
      nextPageId = rightpage.deleteWithCondition(transactionId, condition, recordsNeedToUpdate);

      if (updateSingleAndOnlyPrimary && recordsNeedToUpdate.size() > 1) {
        for (int i = 0; i < maxSize; i++) {
          rootPage.scanTreeAndDeleteRecordWithKey(
              transactionId, recordsNeedToUpdate.get(i).primaryKeyValues);
        }
        for (int i = maxSize; i < recordsNeedToUpdate.size(); i++) {
          /* recover delete */
          IndexPage.RecordInPage record = recordsNeedToUpdate.get(i);
          record.unsetDeleted();
          record.write(transactionId, rightpage, record.myOffset);
        }
        return false;
      } else {
        maxSize = recordsNeedToUpdate.size();
      }
    }

    return updateRecordList(recordsNeedToUpdate);
  }

  public ValueWrapper getRecordInPageValue(IndexPage.RecordInPage record, int primary) {
    return primary < 0
        ? record.nonPrimaryKeyValues[-primary - 1]
        : record.primaryKeyValues[primary];
  }

  public boolean doUpdate(long transactionId, Table.TableMetadata table) throws Exception {
    this.transactionId = transactionId;
    initialization(table);
    //    System.out.println("update initialization finished.");

    conflictingUpdate = columnToSet.primary >= 0;
    updateSingleAndOnlyPrimary = table.getPrimaryKeyNumber() == 1 && columnToSet.primary == 0;

    /* set non-primary key. */
    if (!useWhere) {
      updateCondition();
    } else {
      if (table.getPrimaryKeyNumber() == 1
          && L_where.columnName().getText().equals(table.getPrimaryKeyList().get(0))) {

        // 单列主键，且WHERE子句查询主键时
        if (cmp_where.EQ() != null) return updateWherePrimaryEqual();
        else if (cmp_where.LE() != null || cmp_where.LT() != null) return updateWherePrimaryLess();
        else if (cmp_where.GE() != null || cmp_where.GT() != null)
          return updateWherePrimaryGreater();
        else if (cmp_where.NE() != null) return updateCondition();
        else return true;

      } else {
        return updateCondition();
      }
    }
    return true;
  }

  public void init_where(SQLParser.ConditionContext cond) {
    if (!useWhere) return;
    L_where = cond.expression(0).comparer().columnFullName();
    R_where = cond.expression(1).comparer().literalValue();
    cmp_where = cond.comparator();
  }

  public UpdatePlan(
      String tableName,
      String columnNameToSet,
      String valueLiteralToSet,
      SQLParser.ConditionContext condition_where,
      boolean useWhere) {
    super(LogicalPlanType.UPDATE);
    this.tableName = tableName;
    this.columnNameToSet = columnNameToSet;
    this.valueLiteralToSet = valueLiteralToSet;
    this.useWhere = useWhere;
    init_where(condition_where);
  }
}
