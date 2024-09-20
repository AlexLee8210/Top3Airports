package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntPairWritable> {

	// Create a hadoop text object to store words
	private Text word = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		// System.out.println(value.toString());
		try {
			String[] row = value.toString().split(",");
			word.set(row[4]);

			int count1 = 1;
			int count2 = Integer.parseInt(row[11]);
			// System.out.println("\n----- COUNTER1 -----\n" + count2);

			IntPairWritable counter = new IntPairWritable(count1, count2);
			// System.out.println(counter);

			// System.out.println("\n---------------------------\n" + counter.get(1));
			
			// ((IntWritable)counter.get(1)).set(Integer.parseInt(row[11]));
			context.write(word, counter);
			// System.out.println("\n---------\n" + counter.toString());
		} catch (Exception e) {
			// e.printStackTrace();
		}
	}
}