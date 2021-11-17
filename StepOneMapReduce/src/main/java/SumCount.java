import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class SumCount implements WritableComparable<SumCount> {

    DoubleWritable sum;
    IntWritable count;

    public SumCount(DoubleWritable sum, IntWritable count) {
        this.sum = sum;
        this.count = count;
    }

    public SumCount() {
        set(new DoubleWritable(0), new IntWritable(0));
    }

    public void set(DoubleWritable doubleWritable, IntWritable intWritable) {
        sum = doubleWritable;
        count = intWritable;
    }

    public DoubleWritable getSum() {
        return sum;
    }

    public IntWritable getCount() {
        return count;
    }

    public void addSumCount(SumCount sumCount) {
        set(
                new DoubleWritable(sum.get() + sumCount.getSum().get()),
                new IntWritable(count.get() + sumCount.getCount().get())
        );
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        sum.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sum.readFields(dataInput);
        count.readFields(dataInput);
    }

    @Override
    public int compareTo(SumCount sumCount) {
        int comparision = sum.compareTo(sumCount.sum);
        if(comparision != 0){
            return comparision;
        }
        return count.compareTo(sumCount.count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SumCount sumCount = (SumCount) o;
        return Objects.equals(sum, sumCount.sum) && Objects.equals(count, sumCount.count);
    }

    @Override
    public int hashCode() {
        int result = sum.hashCode();
        result = 31 * result + count.hashCode();
        return result;
    }
}
