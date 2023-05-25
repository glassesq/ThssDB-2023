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
import org.stringtemplate.v4.AutoIndentWriter;

import java.util.ArrayList;

import static java.lang.Math.max;

public class SelectPlan extends LogicalPlan {
  public boolean broken = false;
  public boolean useWhere, useJoin;
  public ArrayList<String> tableNames;
  public ArrayList<SQLParser.ColumnFullNameContext> columns;
  public ArrayList<Column> colInTable;
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

  public void initialization(ArrayList<Table.TableMetadata> tables) {
    System.out.println("initailization start");
    res = new QueryResult();
    colInTable = new ArrayList<>();
    for (SQLParser.ColumnFullNameContext column : columns) {
      res.columns.add(column.getText());
      System.out.println(tables.get(0).name);
      for (Table.TableMetadata table : tables)
        if (tables.size() == 1 || table.name.equals(column.tableName().getText())) {
          String keyName = column.columnName().getText();
          System.out.println(column.getText());
          if (table.columnNames.get(keyName) == null)
            throw new IllegalArgumentException(
                "Column '" + keyName + "' not found in table '" + table.name + "'");
          colInTable.add(table.getColumnDetailByName(keyName));
          System.out.println(table.getColumnDetailByName(keyName).toString());
          break;
        }
    }
    System.out.println("init-1");
    Table.TableMetadata table = !useJoin ? tables.get(0) : null;
    int i = 0;
    for (Table.TableMetadata t : tables) {
      if (useWhere && useJoin && t.name.equals(L_where.tableName().getText())) table = t;
      if (useJoin) {
        if (t.name.equals(L_on.tableName().getText())) {
          String keyName = L_on.columnName().getText();
          if (t.columnNames.get(keyName) == null)
            throw new IllegalArgumentException(
                "Column '" + keyName + "' not found in table '" + t.name + "'");
          L_index = i;
          L_queryCol = t.getColumnDetailByName(keyName);
        }
        if (t.name.equals(R_on.tableName().getText())) {
          String keyName = R_on.columnName().getText();
          if (t.columnNames.get(keyName) == null)
            throw new IllegalArgumentException(
                "Column '" + keyName + "' not found in table '" + t.name + "'");
          R_index = i;
          R_queryCol = t.getColumnDetailByName(keyName);
        }
      }
      ++i;
    }
    if (!useWhere) return;

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

  public ArrayList<String> applyProjection(RecordLogical record) {
    ArrayList<String> result = new ArrayList<>();
    System.out.println("---Proj---");
    System.out.println(columns.size());
    for (int i = 0; i < columns.size(); ++i) {
      int col = colInTable.get(i).primary;
      if (col >= 0) result.add(record.primaryKeyValues[col].toString());
      else result.add(record.nonPrimaryKeyValues[-col-1].toString());
      System.out.println(colInTable.get(i).toString());
      System.out.println(result.get(i));
    }
    System.out.println("+++Proj+++");
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
      do {
        IndexPage page = (IndexPage) IO.read(table.spaceId, pageIter.left);
        pageIter = page.getAllRecordLogical(transactionId);
        if (addRowsWithLess(pageIter, table)) return res;
      } while (pageIter.left > 0);
    }
    return res;
  }

  private boolean addRowsWithLess(
      Pair<Integer, ArrayList<RecordLogical>> pageIter, Table.TableMetadata table) {
    for (RecordLogical record : pageIter.right) {
      if (record.primaryKeyValues[0].compareTo(queryValue) < 0)
        res.rows.add(applyProjection(record));
      else {
        if (cmp_where.LE() != null) res.rows.add(applyProjection(record));
        return true;
      }
    }
    return false;
  }

  public QueryResult getGreater(Table.TableMetadata table) throws Exception {
    IndexPage rootPage =
        (IndexPage) IO.read(table.spaceId, ServerRuntime.config.indexRootPageIndex);
    ValueWrapper[] query = {queryValue};
    Pair<Integer, ArrayList<RecordLogical>> pageIter =
        rootPage.scanTreeAndReturnPage(transactionId, query);

    IndexPage page;
    do {
      addRowsWithCondition(pageIter, table);
      if (pageIter.left <= 0) break;
      page = (IndexPage) IO.read(table.spaceId, pageIter.left);
      pageIter = page.getAllRecordLogical(transactionId);
    } while (true);

    return res;
  }

  public QueryResult getEqual(Table.TableMetadata table) throws Exception {
    IndexPage rootPage =
        (IndexPage) IO.read(table.spaceId, ServerRuntime.config.indexRootPageIndex);
    ValueWrapper[] query = {queryValue};
    Pair<Boolean, IndexPage.RecordInPage> key =
        rootPage.scanTreeAndReturnRecord(transactionId, query);
    if (key.left) applyProjection(new RecordLogical(key.right, table));
    return res;
  }

  public boolean checkCondition(ValueWrapper A, ValueWrapper B, SQLParser.ComparatorContext cmp) {
    if (cmp.NE() != null && A.compareTo(B) != 0) return true;
    if (cmp.EQ() != null && A.compareTo(B) == 0) return true;
    if (cmp.LE() != null && A.compareTo(B) <= 0) return true;
    if (cmp.LT() != null && A.compareTo(B) < 0) return true;
    if (cmp.GE() != null && A.compareTo(B) >= 0) return true;
    return cmp.GT() != null && A.compareTo(B) > 0;
  }
  // 多列主键/非主键/单列主键，查询不等于
  public QueryResult getCondition(Table.TableMetadata table) throws Exception {
    System.out.println("getCondtion!");
    IndexPage rootPage =
        (IndexPage) IO.read(table.spaceId, ServerRuntime.config.indexRootPageIndex);
    Pair<Integer, ArrayList<RecordLogical>> pageIter = rootPage.getLeftmostDataPage(transactionId);
    if (pageIter.left == 0) {
      addRowsWithCondition(pageIter, table);
    } else {
      do {
        IndexPage page = (IndexPage) IO.read(table.spaceId, pageIter.left);
        pageIter = page.getAllRecordLogical(transactionId);
        addRowsWithCondition(pageIter, table);
      } while (pageIter.left > 0);
    }
    return res;
  }

  private void addRowsWithCondition(
      Pair<Integer, ArrayList<RecordLogical>> pageIter, Table.TableMetadata table) {
    for (RecordLogical record : pageIter.right) {
      if (useWhere) {
        ValueWrapper recordValue =
            queryCol.primary < 0
                ? record.nonPrimaryKeyValues[-queryCol.primary - 1]
                : record.primaryKeyValues[queryCol.primary];
        if (!checkCondition(recordValue, queryValue, cmp_where)) continue; // 不满足条件
      }
      res.rows.add(applyProjection(record));
    }
  }

  // 有JOIN的情况
  public void enumPages(
      ArrayList<Table.TableMetadata> tables,
      int iter,
      ArrayList<Pair<Table.TableMetadata, IndexPage>> pages)
      throws Exception {
    if (iter == tables.size()) {
      enumTuple(pages, 0, new ArrayList<>());
      return;
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
      } while (true);
    }
  }

  private void enumTuple(
      ArrayList<Pair<Table.TableMetadata, IndexPage>> pages,
      int iter,
      ArrayList<Pair<Table.TableMetadata, RecordLogical>> records) {
    if (iter == pages.size()) {
      ArrayList<String> result = new ArrayList<>();
      for (SQLParser.ColumnFullNameContext column : columns)
        for (Pair<Table.TableMetadata, RecordLogical> record : records) {
          Table.TableMetadata table = record.left;
          if (table.name.equals(column.tableName().getText())) {
            Column col = table.getColumnDetailByName(column.columnName().getText());
            if (col.primary >= 0) result.add(record.right.primaryKeyValues[col.primary].toString());
            else result.add(record.right.nonPrimaryKeyValues[-col.primary - 1].toString());
          }
        }
      res.rows.add(result);
      return;
    }
    Pair<Table.TableMetadata, IndexPage> page = pages.get(iter);
    ArrayList<RecordLogical> allRecordLogical = page.right.getAllRecordLogical(transactionId).right;
    for (RecordLogical record : allRecordLogical) {
      if (useWhere)
        if (L_where.tableName().equals(page.left.name)) {
          ValueWrapper recordValue =
              queryCol.primary < 0
                  ? record.nonPrimaryKeyValues[-queryCol.primary - 1]
                  : record.primaryKeyValues[queryCol.primary];
          if (!checkCondition(recordValue, queryValue, cmp_where)) continue;
        }
      if (useJoin) {
        if (iter == max(L_index, R_index)) {
          records.add(new Pair<>(page.left, record));
          RecordLogical L_record = records.get(L_index).right,
              R_record = records.get(R_index).right;
          records.remove(iter);
          ValueWrapper L_Value =
              L_queryCol.primary < 0
                  ? L_record.nonPrimaryKeyValues[-L_queryCol.primary - 1]
                  : L_record.primaryKeyValues[L_queryCol.primary];
          ValueWrapper R_Value =
              R_queryCol.primary < 0
                  ? R_record.nonPrimaryKeyValues[-R_queryCol.primary - 1]
                  : R_record.primaryKeyValues[R_queryCol.primary];
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

  public QueryResult getResult(long transactionId, ArrayList<Table.TableMetadata> tables)
      throws Exception {
    this.transactionId = transactionId;
    initialization(tables);
    System.out.println("initialization finished.");
    System.out.println(useJoin);
    System.out.println(useWhere);
    if (!useJoin) {
      if (!useWhere) {
        return getCondition(tables.get(0));
      } else {
        Table.TableMetadata table = tables.get(0);
        if (table.getPrimaryKeyNumber() == 1
            && L_where.columnName().getText().equals(table.getPrimaryKeyList().get(0))) {
          // 单列主键，且WHERE子句查询主键时
          if (cmp_where.EQ() != null) return getEqual(table);
          else if (cmp_where.LE() != null || cmp_where.LT() != null) return getLess(table);
          else if (cmp_where.GE() != null || cmp_where.GT() != null) return getGreater(table);
          else if (cmp_where.NE() != null) return getCondition(table);
          else return null;
        } else return getCondition(tables.get(0));
      }
    } else return getJoin(tables);
  }

  public void init_on(SQLParser.ConditionContext cond) {
    if (!useJoin) return;
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

  public SelectPlan(
      ArrayList<SQLParser.ColumnFullNameContext> columns,
      ArrayList<String> tableNames,
      SQLParser.ConditionContext condition_on,
      SQLParser.ConditionContext condition_where,
      boolean useJoin,
      boolean useWhere) {
    super(LogicalPlanType.SELECT);
    this.columns = columns;
    this.tableNames = tableNames;
    this.useWhere = useWhere;
    this.useJoin = useJoin;
    init_where(condition_where);
    init_on(condition_on);
  }
}
