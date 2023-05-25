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
    int primaryKeyNumber = metadata.getPrimaryKeyNumber();
    int nonPrimaryKeyNumber = metadata.getNonPrimaryKeyNumber();
    for (int i = 0; i < primaryKeyNumber; i++) {
      primaryKeyValues[i] = new ValueWrapper(record.primaryKeyValues[i]);
    }
    for (int i = 0; i < nonPrimaryKeyNumber; i++) {
      nonPrimaryKeyValues[i] = new ValueWrapper(record.nonPrimaryKeyValues[i]);
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
