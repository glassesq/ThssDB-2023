package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.sql.SQLParser;

import javax.management.Query;
import java.util.ArrayList;

public class SelectPlan extends LogicalPlan {
  public boolean broken = false;
  public boolean useWhere = false, useOn = false, useJoin = false;
  public ArrayList<String> tableNames;
  public ArrayList<SQLParser.ColumnFullNameContext> columns;
  public SQLParser.ColumnFullNameContext L_on, R_on;
  public SQLParser.ComparatorContext cmp_on;
  public SQLParser.ColumnFullNameContext L_where;
  public SQLParser.LiteralValueContext R_where;
  public SQLParser.ComparatorContext cmp_where;
  public long transactionId = -1;

  //无JOIN的情况
  //单列主键，且WHERE子句查询主键时，使用如下方法进行查询
  public QueryResult getLess(Table.TableMetadata table) {
    QueryResult res = new QueryResult();
    // TODO 遍历直到等于
    return res;
  }
  public QueryResult getGreater(Table.TableMetadata table) {
    QueryResult res = new QueryResult();
    // TODO 先getEqual再向后遍历
    return res;
  }
  public QueryResult getEqual(Table.TableMetadata table) {
    QueryResult res = new QueryResult();
    // TODO b link tree 上查询等于
    return res;
  }

  // 多列主键/非主键/单列主键，查询不等于
  public QueryResult getCondition(Table.TableMetadata table) {
    QueryResult res = new QueryResult();
    // TODO 遍历取出符合条件的
    return res;
  }
  // 有JOIN的情况
  public QueryResult getJoin(ArrayList<Table.TableMetadata> tables) {
    QueryResult res = new QueryResult();
    // TODO 块嵌套
    return res;
  }
  public QueryResult getResult(long transactionId, ArrayList<Table.TableMetadata> tables) {
    this.transactionId = transactionId;
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
