package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class WordAndCount implements Comparable<WordAndCount> {

        private final Text word;
        private final IntPairWritable tup;

        public WordAndCount(Text word, IntPairWritable tup) {
            this.word = word;
            this.tup = tup;
        }

        public Text getWord() {
            return word;
        }

        public IntPairWritable getTup() {
            return tup;
        }

        public IntWritable getNumFlights() {
            return (IntWritable) tup.get(0);
        }

        public void addFlight() {
            getNumFlights().set(getNumFlights().get() + 1);
        }

        public void addDelay(int delay) {
            // System.out.println("\n-_--_----\n" + delay);
            getDelay().set(getDelay().get() + delay);
        }

        public IntWritable getDelay() {
            return (IntWritable) tup.get(1);
        }

        public FloatWritable getDelayRatio() {
            return new FloatWritable(((float) getDelay().get()) / ((float) getNumFlights().get()));
        }

    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
        @Override
        public int compareTo(WordAndCount other) {
            return Float.compare(getDelayRatio().get(), other.getDelayRatio().get());
            // float diff = count.get() - other.count.get();
            // if (diff > 0) {
            //     return 1;
            // } else if (diff < 0) {
            //     return -1;
            // }
            // return 0;
        }


        public String toString(){
            return "(" + word.toString() + " , " + getNumFlights().toString() + " , " + getDelay().toString() + ")";
            // return "("+word.toString() +" , "+ count.toString()+")";
        }
    }

