import lombok.*;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class CustomOutputValue implements WritableComparable<CustomOutputValue> {

    private int sum;
    private int count;

    public void readFields(DataInput in) throws IOException {
        this.sum = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.sum);
    }

    public boolean equals(Object o) {
        if (!(o instanceof CustomOutputValue)) {
            return false;
        } else {
            CustomOutputValue other = (CustomOutputValue) o;
            return this.sum == other.sum && this.count == other.count;
        }
    }

    public int hashCode() {
        return this.sum * count;
    }

    public String toString() {
        return this.sum + ", " + this.count ;
    }

    @Override
    public int compareTo(CustomOutputValue o) {
        return 0;
    }
}

