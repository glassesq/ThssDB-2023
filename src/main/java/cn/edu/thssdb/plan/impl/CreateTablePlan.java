package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Table;

public class CreateTablePlan extends LogicalPlan {

    public Table.TableMetadata tableMetadata;

    public CreateTablePlan(Table.TableMetadata metadata) {
        super(LogicalPlanType.CREATE_TABLE);
        this.tableMetadata = metadata;
    }

    @Override
    public String toString() {
        return "CreateTablePlan{" + "databaseName='" + tableMetadata.name + "'}";
    }
}
