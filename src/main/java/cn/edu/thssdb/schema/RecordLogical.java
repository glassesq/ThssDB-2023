package cn.edu.thssdb.schema;

import cn.edu.thssdb.storage.page.IndexPage;

public class RecordLogical {
    public ValueWrapper[] primaryKeyValues;
    public ValueWrapper[] nonPrimaryKeyValues;

    public RecordLogical(Table.TableMetadata metadata) {
        primaryKeyValues = new ValueWrapper[metadata.getPrimaryKeyNumber()];
        nonPrimaryKeyValues = new ValueWrapper[metadata.getNonPrimaryKeyNumber()];
    }

    public RecordLogical(IndexPage.RecordInPage record, Table.TableMetadata metadata) {
        primaryKeyValues = new ValueWrapper[metadata.getPrimaryKeyNumber()];
        nonPrimaryKeyValues = new ValueWrapper[metadata.getNonPrimaryKeyNumber()];
        int npIndex = 0;
        for (int i = 0; i < metadata.columnDetails.size(); i++) {
            Column column = metadata.columnDetails.get(i);
            if (column.primary >= 0) {
                primaryKeyValues[column.primary] = new ValueWrapper(record.primaryKeyValues[column.primary]);
            } else {
                nonPrimaryKeyValues[npIndex] = new ValueWrapper(record.nonPrimaryKeyValues[npIndex]);
                ++npIndex;
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (ValueWrapper value : primaryKeyValues) {
            result.append(value);
            result.append(" [primary]\n");
            System.out.println(value.toRawString());
        }
        for (ValueWrapper value : nonPrimaryKeyValues) {
            result.append(value);
            result.append(" [non-primary]\n");
            System.out.println(value.toRawString());
        }
        return result.toString();
    }
}
