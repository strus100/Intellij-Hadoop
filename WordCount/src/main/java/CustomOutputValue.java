import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomOutputValue implements WritableComparable {
    private int value;

    public CustomOutputValue() {
    }

    public CustomOutputValue(int value) {
        this.set(value);
    }

    public void set(int value) {
        this.value = value;
    }

    public int get() {
        return this.value;
    }

    public void readFields(DataInput in) throws IOException {
        this.value = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.value);
    }

    public boolean equals(Object o) {
        if (!(o instanceof CustomOutputValue)) {
            return false;
        } else {
            CustomOutputValue other = (CustomOutputValue) o;
            return this.value == other.value;
        }
    }

    public int hashCode() {
        return this.value;
    }

    public int compareTo(Object o) {
        int thisValue = this.value;
        int thatValue = ((CustomOutputValue)o).value;
        return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }

    public String toString() {
        return Integer.toString(this.value);
    }

    static {
        WritableComparator.define(CustomOutputValue.class, new CustomOutputValue.Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(CustomOutputValue.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int thisValue = readInt(b1, s1);
            int thatValue = readInt(b2, s2);
            return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
        }
    }
}

