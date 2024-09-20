package edu.cs.utexas.HadoopEx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntPairWritable implements Writable {
        private IntWritable value1;
        private IntWritable value2;

        public IntPairWritable() {
            value1 = new IntWritable();
            value2 = new IntWritable();
        }
        
        public IntPairWritable(IntPairWritable ipw) {
            value1 = new IntWritable(ipw.get(0).get());
            value2 = new IntWritable(ipw.get(1).get());
        }

        public IntPairWritable(int value1, int value2) {
            this.value1 = new IntWritable(value1);
            this.value2 = new IntWritable(value2);
        }

        public IntWritable get(int i) {
            return i == 0 ? this.value1 : this.value2;
        }

        @Override
        public String toString() {
            return value1.toString() + "," + value2.toString();
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            value1.readFields(in);
            value2.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            value1.write(out);
            value2.write(out);
        }
}