package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.DataType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ValueWrapper {
  /** raw bytes value */
  public byte[] bytes;

  /** strLength : {@code length of string bytes} */
  private int strLength = 0;

  public boolean isNull;

  public DataType type;

  /**
   * If this value is stored offPage; when {@code offPage} is true, value is set to the
   * corresponding pointer(spaceId, pageId);
   */
  public boolean offPage = false;

  public ValueWrapper(Column column) {
    this.bytes = new byte[column.getLength()];
    this.type = column.type;
    this.offPage = column.offPage;
    this.isNull = true;
  }

  /**
   * deep copy constructor
   *
   * @param bytes bytes to be copied
   * @param type data type
   * @param length maximum length of the bytes (may have zero suffix).
   * @param offPage if the value is stored offPage;
   */
  public ValueWrapper(byte[] bytes, DataType type, int length, boolean offPage) {
    this.isNull = false;
    this.bytes = Arrays.copyOf(bytes, length);
    this.type = type;
    this.offPage = offPage;
    if (this.type == DataType.STRING) {
      this.strLength = Math.min(length, bytes.length);
      for (int i = 0; i < this.strLength; i++) {
        if (bytes[i] == 0) {
          this.strLength = i;
          break;
        }
      }
    }
  }

  /**
   * construct a null value wrapper
   *
   * @param isNull must be true
   * @param type data type
   */
  public ValueWrapper(boolean isNull, DataType type) {
    this.type = type;
    this.strLength = 0;
    this.bytes = new byte[0];
    this.isNull = isNull;
  }

  /**
   * deep copy constructor
   *
   * @param o the object to be copied.
   */
  public ValueWrapper(ValueWrapper o) {
    this.isNull = o.isNull;
    this.bytes = Arrays.copyOf(o.bytes, o.bytes.length);
    this.type = o.type;
    this.offPage = o.offPage;
    this.strLength = o.strLength;
  }

  public void setWithNull(String str) {
    if (str.equals("null")) {
      this.isNull = true;
    } else {
      this.isNull = false;
      if (str.startsWith("'") && str.endsWith("'")) {
        set(str.substring(1, str.length() - 1));
      } else {
        set(str);
      }
    }
  }

  private void set(String string) {
    switch (type) {
      case INT:
        this.bytes = new byte[4];
        int value = Integer.parseInt(string);
        bytes[0] = (byte) (value >> 24);
        bytes[1] = (byte) (value >> 16);
        bytes[2] = (byte) (value >> 8);
        bytes[3] = (byte) value;
        break;
      case DOUBLE:
        this.bytes = new byte[8];
        /* we treat different type of NaNs as the same. */
        long doubleValue = Double.doubleToLongBits(Double.parseDouble(string));
        bytes[0] = (byte) (doubleValue >> 56);
        bytes[1] = (byte) (doubleValue >> 48);
        bytes[2] = (byte) (doubleValue >> 40);
        bytes[3] = (byte) (doubleValue >> 32);
        bytes[4] = (byte) (doubleValue >> 24);
        bytes[5] = (byte) (doubleValue >> 16);
        bytes[6] = (byte) (doubleValue >> 8);
        bytes[7] = (byte) doubleValue;
        break;
      case FLOAT:
        this.bytes = new byte[4];
        int floatValue = Float.floatToIntBits(Float.parseFloat(string));
        bytes[0] = (byte) (floatValue >> 24);
        bytes[1] = (byte) (floatValue >> 16);
        bytes[2] = (byte) (floatValue >> 8);
        bytes[3] = (byte) floatValue;
        break;
      case LONG:
        this.bytes = new byte[8];
        long longValue = Long.parseLong(string);
        bytes[0] = (byte) (longValue >> 56);
        bytes[1] = (byte) (longValue >> 48);
        bytes[2] = (byte) (longValue >> 40);
        bytes[3] = (byte) (longValue >> 32);
        bytes[4] = (byte) (longValue >> 24);
        bytes[5] = (byte) (longValue >> 16);
        bytes[6] = (byte) (longValue >> 8);
        bytes[7] = (byte) longValue;
        break;
      case STRING:
        byte[] stringBytes = string.getBytes(StandardCharsets.UTF_8);
        this.bytes = new byte[stringBytes.length];
        System.arraycopy(stringBytes, 0, bytes, 0, stringBytes.length);
        strLength = stringBytes.length;
        break;
    }
  }

  public Integer compareTo(ValueWrapper o) {
    if (isNull || o.isNull) return null;
    switch (type) {
      case INT:
        return Integer.compare(parseIntegerBig(), o.parseIntegerBig());
      case LONG:
        return Long.compare(parseLongBig(), o.parseLongBig());
      case FLOAT:
        return Float.compare(
            Float.intBitsToFloat(parseIntegerBig()), Float.intBitsToFloat(o.parseIntegerBig()));
      case DOUBLE:
        return Double.compare(
            Double.longBitsToDouble(parseLongBig()), Double.longBitsToDouble((o.parseLongBig())));
      case STRING:
      default:
        return this.toString().compareTo(o.toString());
    }
  }

  public static Integer compareArray(ValueWrapper[] a, ValueWrapper[] b) {
    int i;
    for (i = 0; i < Math.min(a.length, b.length); i++) {
      if (a[i].compareTo(b[i]) == null || a[i].compareTo(b[i]) != 0) break;
    }
    if (i < Math.min(a.length, b.length)) return a[i].compareTo(b[i]);
    return a.length - b.length;
  }

  public String toRawString() {
    StringBuilder result = new StringBuilder();
    for (byte b : this.bytes) {
      result.append(String.format("%02x ", b));
    }
    return result.toString();
  }

  public int parseIntegerBig() {
    return ((bytes[0] & 0xFF) << 24)
        | ((bytes[1] & 0xFF) << 16)
        | ((bytes[2] & 0xFF) << 8)
        | (bytes[3] & 0xFF);
  }

  public long parseLongBig() {
    return Integer.toUnsignedLong(
                ((bytes[0] & 0xFF) << 24)
                    | ((bytes[1] & 0xFF) << 16)
                    | ((bytes[2] & 0xFF) << 8)
                    | (bytes[3] & 0xFF))
            << 32
        | ((Integer.toUnsignedLong(bytes[4] & 0xFF) << 24)
            | ((bytes[5] & 0xFF) << 16)
            | ((bytes[6] & 0xFF) << 8)
            | (bytes[7] & 0xFF));
  }

  @Override
  public String toString() {
    if (isNull) return "null";
    switch (this.type) {
      case INT:
        return String.valueOf(parseIntegerBig());
      case LONG:
        return String.valueOf(parseLongBig());
      case DOUBLE:
        return String.valueOf(Double.longBitsToDouble(parseLongBig()));
      case FLOAT:
        return String.valueOf(Float.intBitsToFloat(parseIntegerBig()));
      case STRING:
      default:
        return new String(bytes, 0, strLength, StandardCharsets.UTF_8);
    }
  }
}
