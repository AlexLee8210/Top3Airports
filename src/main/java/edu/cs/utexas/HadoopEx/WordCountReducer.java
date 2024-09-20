package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, IntPairWritable, Text, IntPairWritable> {

   public void reduce(Text text, Iterable<IntPairWritable> values, Context context)
           throws IOException, InterruptedException {
	   
        WordAndCount wac = new WordAndCount(text, new IntPairWritable(0,0));

        
        // System.out.println("\n-------- REDUCER ----------\n" + text.toString());

        // size of values is 1 because key only has one distinct value
        for (IntPairWritable value : values) {
            // counter = counter + 1;
            // System.out.println(value.toString());
            
			// System.out.println("\n---------------------------\n" + value.get(0));
            wac.addFlight();
            wac.addDelay(value.get(1).get());
        }
        context.write(text, wac.getTup());
   }
}