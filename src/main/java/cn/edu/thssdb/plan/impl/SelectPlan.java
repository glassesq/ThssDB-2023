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
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;

public class SelectPlan extends LogicalPlan {
  public boolean broken = false;
  public boolean useWhere, useOn, useJoin;
  public ArrayList<String> tableNames;
  public ArrayList<SQLParser.ColumnFullNameContext> columns;
  public SQLParser.ColumnFullNameContext L_on, R_on;
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
    for (Table.TableMetadata t : tables)
      if (t.name.equals(L_where.tableName().getText()))
        table = t;
    if (table == null) throw new Exception(); // TODO 异常处理
    queryCol = table.columnDetails.get(table.columns.get(keyName));
    queryValue = new ValueWrapper(queryCol);
    queryValue.setWithNull(R_where.getText());
  }

  public ArrayList<ValueWrapper> applyProjection(RecordLogical record, Table.TableMetadata table) {
    ArrayList<ValueWrapper> result = new ArrayList<>();
    for (SQLParser.ColumnFullNameContext column : columns) {
      Column col = table.columnDetails.get(table.columns.get(column.columnName().getText()));
      if (col.primary >= 0) result.add(record.primaryKeyValues[col.primary]);
      else result.add(record.nonPrimaryKeyValues[-col.primary-1]);
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
  public boolean checkCondition(ValueWrapper A, ValueWrapper B) {
    if (cmp_where.NE() != null && A.compareTo(B) != 0) return true;
    if (cmp_where.EQ() != null && A.compareTo(B) == 0) return true;
    if (cmp_where.LE() != null && A.compareTo(B) < 0) return true;
    if (cmp_where.LT() != null && A.compareTo(B) <= 0) return true;
    if (cmp_where.GE() != null && A.compareTo(B) > 0) return true;
    return cmp_where.GT() != null && A.compareTo(B) >= 0;
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
        if (!checkCondition(recordValue, queryValue)) continue; // 不满足条件
      }
      res.rows.add(applyProjection(record, table));
    }
  }

  // 有JOIN的情况
  public QueryResult getJoin(ArrayList<Table.TableMetadata> tables) {
    // TODO 块嵌套
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
