package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.DataType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class RecordLogical {
    public static class ValueWrapper implements Comparable<ValueWrapper> {
        /**
         * raw bytes value
         */
        public byte[] bytes;
        public DataType type;
        /**
         * If this value is stored offPage;
         * when {@code offPage} is true, value is set to the corresponding pointer(spaceId, pageId);
         */
        public boolean offPage = false;

        /**
         * deep copy constructor
         *
         * @param bytes   bytes to be copied
         * @param type    data type
         * @param offPage if the value is stored offPage;
         */
        public ValueWrapper(byte[] bytes, DataType type, boolean offPage) {
            this.bytes = Arrays.copyOf(bytes, bytes.length);
            this.type = type;
            this.offPage = offPage;
        }

        /**
         * deep copy constructor
         *
         * @param o the object to be copied.
         */
        public ValueWrapper(ValueWrapper o) {
            this.bytes = Arrays.copyOf(o.bytes, o.bytes.length);
            this.type = o.type;
            this.offPage = o.offPage;
        }

        public void set(String string) throws Exception {
            // TODO: exception
            switch (type) {
                case INT:
                    int value = Integer.parseInt(string);
                    bytes[0] = (byte) (value >> 24);
                    bytes[1] = (byte) (value >> 16);
                    bytes[2] = (byte) (value >> 8);
                    bytes[3] = (byte) value;
                    break;
                case DOUBLE:
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
                    int floatValue = Float.floatToIntBits(Float.parseFloat(string));
                    bytes[0] = (byte) (floatValue >> 24);
                    bytes[1] = (byte) (floatValue >> 16);
                    bytes[2] = (byte) (floatValue >> 8);
                    bytes[3] = (byte) floatValue;
                    break;

                case LONG:
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
            }
        }

        @Override
        public int compareTo(ValueWrapper o) {
            // TODO: compare for DOUBLE and FLOAT type.
            switch (type) {
                case INT:
                    return Integer.compare(parseIntegerBig(), o.parseIntegerBig());
                case LONG:
                    return Long.compare(parseLongBig(), o.parseLongBig());
                case FLOAT:
                    return Float.compare(Float.intBitsToFloat(parseIntegerBig()), Float.intBitsToFloat(o.parseIntegerBig()));
                case DOUBLE:
                    return Double.compare(Double.longBitsToDouble(parseLongBig()), Double.longBitsToDouble((o.parseLongBig())));
                case STRING:
                    return this.toString().compareTo(o.toString());
                default:
                    return 0;
            }
        }

        public String toRawString() {
            StringBuilder result = new StringBuilder();
            for (byte b : this.bytes) {
                result.append(String.format("%02x ", b));
            }
            return result.toString();
        }

        public int parseIntegerBig() {
            return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
        }


        // TODO: test for unsigned and signed stuff
        public long parseLongBig() {
            return Integer.toUnsignedLong(((bytes[0] & 0xFF) << 24) |
                    ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF)) << 32
                    | ((Integer.toUnsignedLong(bytes[4] & 0xFF) << 24) | ((bytes[5] & 0xFF) << 16)
                    | ((bytes[6] & 0xFF) << 8) | (bytes[7] & 0xFF));
        }

        @Override
        public String toString() {
            switch (this.type) {
                case INT:
                    return String.valueOf(parseIntegerBig());
                case LONG:
                    return String.valueOf(parseLongBig());
                case STRING:
                    return new String(bytes, StandardCharsets.UTF_8);
                case DOUBLE:
                    return String.valueOf(Double.longBitsToDouble(parseLongBig()));
                case FLOAT:
                    return String.valueOf(Float.intBitsToFloat(parseIntegerBig()));
            }
            return "";
        }
    }

    public ArrayList<ValueWrapper> values = new ArrayList<>();
}
