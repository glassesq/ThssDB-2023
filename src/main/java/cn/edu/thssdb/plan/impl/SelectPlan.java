package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.communication.IO;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.runtime.ServerRuntime;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.RecordLogical;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.ValueWrapper;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.storage.page.IndexPage;
import cn.edu.thssdb.storage.page.Page;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.Comparator;

import static java.lang.Math.max;

public class SelectPlan extends LogicalPlan {
  public boolean broken = false;
  public boolean useWhere, useOn, useJoin;
  public ArrayList<String> tableNames;
  public ArrayList<SQLParser.ColumnFullNameContext> columns;
  public SQLParser.ColumnFullNameContext L_on, R_on;
  public int L_index, R_index;
  public Column L_queryCol, R_queryCol;
  public SQLParser.ComparatorContext cmp_on;
  public SQLParser.ColumnFullNameContext L_where;
  public SQLParser.LiteralValueContext R_where;
  public ValueWrapper queryValue;
  public Column queryCol;

  public SQLParser.ComparatorContext cmp_where;
  public QueryResult res;
  public long transactionId = -1;

  public void initialization(ArrayList<Table.TableMetadata> tables) throws Exception {
    res = new QueryResult();
    for (SQLParser.ColumnFullNameContext column: columns)
      res.columns.add(column.getText());

    if (!useWhere) return;
    String keyName = L_where.columnName().getText();
    Table.TableMetadata table = null;
    int i = 0;
    for (Table.TableMetadata t : tables) {
      if (t.name.equals(L_where.tableName().getText()))
        table = t;
      if (useOn) {
        if (t.name.equals(L_on.tableName().getText())) {
          L_index = i;
          L_queryCol = t.columnDetails.get(t.columns.get(L_on.columnName().getText()));
        }
        if (t.name.equals(R_on.tableName().getText())) {
          R_index = i;
          R_queryCol = t.columnDetails.get(t.columns.get(R_on.columnName().getText()));
        }
      }
      ++i;
    }
    if (table == null) throw new Exception(); // TODO 异常处理
    queryCol = table.columnDetails.get(table.columns.get(keyName));
    queryValue = new ValueWrapper(queryCol);
    queryValue.setWithNull(R_where.getText());
  }

  public ArrayList<String> applyProjection(RecordLogical record, Table.TableMetadata table) {
    ArrayList<String> result = new ArrayList<>();
    for (SQLParser.ColumnFullNameContext column : columns) {
      Column col = table.columnDetails.get(table.columns.get(column.columnName().getText()));
      if (col.primary >= 0) result.add(record.primaryKeyValues[col.primary].toString());
      else result.add(record.nonPrimaryKeyValues[-col.primary-1].toString());
    }
    return result;
  }

  // 无JOIN的情况
  // 单列主键，且WHERE子句查询主键时，使用如下方法进行查询
  public QueryResult getLess(Table.TableMetadata table) throws Exception {
    IndexPage rootPage =
            (IndexPage) IO.read(table.spaceId, ServerRuntime.config.indexRootPageIndex);
    Pair<Integer, ArrayList<RecordLogical>> pageIter = rootPage.getLeftmostDataPage(transactionId);

    if (pageIter.left == 0) {
      if (addRowsWithLess(pageIter, table)) return res;
    } else {
      IndexPage page = (IndexPage) IO.read(table.spaceId, pageIter.left);
      do {
        pageIter = page.getAllRecordLogical(transactionId);
        if (addRowsWithLess(pageIter, table)) return res;
        if (pageIter.left <= 0) break;
        page = (IndexPage) IO.read(table.spaceId, pageIter.left);
      }while(true);
    }
    return res;
  }

  private boolean addRowsWithLess(Pair<Integer, ArrayList<RecordLogical>> pageIter, Table.TableMetadata table) {
    for (RecordLogical record : pageIter.right) {
      if (record.primaryKeyValues[0].compareTo(queryValue) < 0)
        res.rows.add(applyProjection(record, table));
      else {
        if (cmp_where.LT() != null) res.rows.add(applyProjection(record, table));
        return true;
      }
    }
    return false;
  }

  public QueryResult getGreater(Table.TableMetadata table) throws Exception {
    IndexPage rootPage =
            (IndexPage) IO.read(table.spaceId, ServerRuntime.config.indexRootPageIndex);
    ValueWrapper[] query = {queryValue};
    Pair<Integer, ArrayList<RecordLogical>> pageIter = rootPage.scanTreeAndReturnPage(transactionId, query);

    IndexPage page;
    do {
      addRowsWithCondition(pageIter, table);
      if (pageIter.left <= 0) break;
      page = (IndexPage) IO.read(table.spaceId, pageIter.left);
      pageIter = page.getAllRecordLogical(transactionId);
    }while(true);

    return res;
  }
  public QueryResult getEqual(Table.TableMetadata table) throws Exception {
    IndexPage rootPage =
            (IndexPage) IO.read(table.spaceId, ServerRuntime.config.indexRootPageIndex);
    ValueWrapper[] query = {queryValue};
    Pair<Boolean, IndexPage.RecordInPage> key = rootPage.scanTreeAndReturnRecord(transactionId, query);
    if (key.left) applyProjection(new RecordLogical(key.right, table), table);
    return res;
  }
  public boolean checkCondition(ValueWrapper A, ValueWrapper B, SQLParser.ComparatorContext cmp) {
    if (cmp.NE() != null && A.compareTo(B) != 0) return true;
    if (cmp.EQ() != null && A.compareTo(B) == 0) return true;
    if (cmp.LE() != null && A.compareTo(B) < 0) return true;
    if (cmp.LT() != null && A.compareTo(B) <= 0) return true;
    if (cmp.GE() != null && A.compareTo(B) > 0) return true;
    return cmp.GT() != null && A.compareTo(B) >= 0;
  }
  // 多列主键/非主键/单列主键，查询不等于
  public QueryResult getCondition(Table.TableMetadata table) throws Exception {
    IndexPage rootPage =
            (IndexPage) IO.read(table.spaceId, ServerRuntime.config.indexRootPageIndex);
    Pair<Integer, ArrayList<RecordLogical>> pageIter = rootPage.getLeftmostDataPage(transactionId);
    if (pageIter.left == 0) {
      addRowsWithCondition(pageIter, table);
    } else {
      IndexPage page = (IndexPage) IO.read(table.spaceId, pageIter.left);
      do {
        pageIter = page.getAllRecordLogical(transactionId);
        addRowsWithCondition(pageIter, table);
        if (pageIter.left <= 0) break;
        page = (IndexPage) IO.read(table.spaceId, pageIter.left);
      }while(true);
    }
    return res;
  }

  private void addRowsWithCondition(Pair<Integer, ArrayList<RecordLogical>> pageIter, Table.TableMetadata table) {
    for (RecordLogical record : pageIter.right) {
      if (useWhere) {
        ValueWrapper recordValue = queryCol.primary < 0 ? record.nonPrimaryKeyValues[-queryCol.primary-1] : record.primaryKeyValues[queryCol.primary];
        if (!checkCondition(recordValue, queryValue, cmp_where)) continue; // 不满足条件
      }
      res.rows.add(applyProjection(record, table));
    }
  }

  // 有JOIN的情况
  public void enumPages(ArrayList<Table.TableMetadata> tables, int iter, ArrayList<Pair<Table.TableMetadata, IndexPage>> pages) throws Exception {
    if (iter == tables.size()) {
      enumTuple(pages, 0, new ArrayList<>());
      return ;
    }
    Table.TableMetadata table = tables.get(iter);
    IndexPage rootPage =
            (IndexPage) IO.read(table.spaceId, ServerRuntime.config.indexRootPageIndex);
    Pair<Integer, ArrayList<RecordLogical>> pageIter = rootPage.getLeftmostDataPage(transactionId);
    if (pageIter.left == 0) {
      pages.add(new Pair<>(table, rootPage));
      enumPages(tables, iter + 1, pages);
      pages.remove(iter);
    } else {
      IndexPage page = (IndexPage) IO.read(table.spaceId, pageIter.left);
      do {
        pageIter = page.getAllRecordLogical(transactionId);
        pages.add(new Pair<>(table, page));
        enumPages(tables, iter + 1, pages);
        pages.remove(iter);
        if (pageIter.left <= 0) break;
        page = (IndexPage) IO.read(table.spaceId, pageIter.left);
      }while(true);
    }
  }

  private void enumTuple(ArrayList<Pair<Table.TableMetadata, IndexPage>> pages, int iter, ArrayList<Pair<Table.TableMetadata, RecordLogical>> records) {
    if (iter == pages.size()) {
      ArrayList<String> result = new ArrayList<>();
      // TODO 优化
      for (SQLParser.ColumnFullNameContext column : columns)
        for (Pair<Table.TableMetadata, RecordLogical> record: records) {
          Table.TableMetadata table = record.left;
          if (table.name.equals(column.tableName().getText())) {
            Column col = table.columnDetails.get(table.columns.get(column.columnName().getText()));
            if (col.primary >= 0) result.add(record.right.primaryKeyValues[col.primary].toString());
            else result.add(record.right.nonPrimaryKeyValues[-col.primary - 1].toString());
          }
        }
      res.rows.add(result);
      return ;
    }
    Pair<Table.TableMetadata, IndexPage> page = pages.get(iter);
    ArrayList<RecordLogical> allRecordLogical = page.right.getAllRecordLogical(transactionId).right;
    for (RecordLogical record : allRecordLogical) {
      if (useWhere)
        if (L_where.tableName().equals(page.left.name)) {
          ValueWrapper recordValue = queryCol.primary < 0 ? record.nonPrimaryKeyValues[-queryCol.primary-1] : record.primaryKeyValues[queryCol.primary];
          if (!checkCondition(recordValue, queryValue, cmp_where)) continue;
        }
      if (useOn) {
        if (iter == max(L_index, R_index)) {
          records.add(new Pair<>(page.left, record));
          RecordLogical L_record = records.get(L_index).right, R_record = records.get(R_index).right;
          records.remove(iter);
          ValueWrapper L_Value = L_queryCol.primary < 0 ? L_record.nonPrimaryKeyValues[-L_queryCol.primary-1] : L_record.primaryKeyValues[L_queryCol.primary];
          ValueWrapper R_Value = R_queryCol.primary < 0 ? R_record.nonPrimaryKeyValues[-R_queryCol.primary-1] : R_record.primaryKeyValues[R_queryCol.primary];
          if (!checkCondition(L_Value, R_Value, cmp_on)) continue;
        }
      }
      records.add(new Pair<>(page.left, record));
      enumTuple(pages, iter + 1, records);
      records.remove(iter);
    }
  }

  public QueryResult getJoin(ArrayList<Table.TableMetadata> tables) throws Exception {
    enumPages(tables, 0, new ArrayList<>());
    return res;
  }
  public QueryResult getResult(long transactionId, ArrayList<Table.TableMetadata> tables) throws Exception {
    this.transactionId = transactionId;
    initialization(tables);
    if (!useJoin) {
      if (!useWhere) {
        return getCondition(tables.get(0));
      }else {
        Table.TableMetadata table = tables.get(0);
        if (table.getPrimaryKeyNumber() == 1 && L_where.columnName().getText().equals(table.getPrimaryKeyList().get(0))) {
          //单列主键，且WHERE子句查询主键时
          if (cmp_where.EQ() != null) return getEqual(table);
          else if (cmp_where.LE() != null || cmp_where.LT() != null) return getLess(table);
          else if (cmp_where.GE() != null || cmp_where.GT() != null) return getGreater(table);
          else if (cmp_where.NE() != null) return getCondition(table);
          else return null;
        }else return getCondition(tables.get(0));
      }
    }else return getJoin(tables);
  }
  public void init_on(SQLParser.ConditionContext cond) {
    if (!useOn) return;
    L_on = cond.expression(0).comparer().columnFullName();
    R_on = cond.expression(1).comparer().columnFullName();
    cmp_on = cond.comparator();
  }
  public void init_where(SQLParser.ConditionContext cond) {
    if (!useWhere) return;
    L_where = cond.expression(0).comparer().columnFullName();
    R_where = cond.expression(1).comparer().literalValue();
    cmp_where = cond.comparator();
  }

  public SelectPlan(ArrayList<SQLParser.ColumnFullNameContext> columns, ArrayList<String> tableNames, SQLParser.ConditionContext condition_on, SQLParser.ConditionContext condition_where, boolean useJoin, boolean useOn, boolean useWhere) {
    super(LogicalPlanType.SELECT);
    this.columns = columns;
    this.tableNames = tableNames;
    this.useOn = useOn;
    this.useWhere = useWhere;
    this.useJoin = useJoin;
    init_where(condition_where);
    init_on(condition_on);
  }
}
