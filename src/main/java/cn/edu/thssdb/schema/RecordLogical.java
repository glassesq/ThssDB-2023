package cn.edu.thssdb.schema;

import cn.edu.thssdb.storage.page.IndexPage;

import java.util.ArrayList;

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
        ArrayList<Integer> primaryOffsetList = metadata.getPrimaryOffsetInOrder();
        int nonPrimaryOffset = 0;
        int npIndex = 0;
        for (int i = 0; i < metadata.columnDetails.size(); i++) {
            Column column = metadata.columnDetails.get(i);
            byte[] newValue = new byte[column.getLength()];
            if (column.primary >= 0) {
                System.arraycopy(record.primaryKeys, primaryOffsetList.get(column.primary), newValue, 0, column.getLength());
                primaryKeyValues[column.primary] = (new ValueWrapper(newValue, column.type, column.getLength(), column.offPage));
            } else {
                System.arraycopy(record.nonPrimaryKeys, nonPrimaryOffset, newValue, 0, column.getLength());
                nonPrimaryKeyValues[npIndex] = (new ValueWrapper(newValue, column.type, column.getLength(), column.offPage));
                ++npIndex;
                nonPrimaryOffset += column.getLength();
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
