package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class TopKMapper extends Mapper<Text, Text, Text, IntPairWritable> {

	private Logger logger = Logger.getLogger(TopKMapper.class);


	private PriorityQueue<WordAndCount> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		try {
			// System.out.println("\n ------------------- TOP K MAPPER ------------------- \n");
			// System.out.println(key + ": " + value);
			String[] parsed = value.toString().split(",");
			int count = Integer.parseInt(parsed[0]);
			int delay = Integer.parseInt(parsed[1]);

			pq.add(new WordAndCount(new Text(key), new IntPairWritable(count, delay)));

			if (pq.size() > 3) {
				pq.poll();
			}
		} catch (Exception e) {
			// e.printStackTrace();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {


		while (pq.size() > 0) {
			logger.info("TopKMapper PQ Status: " + pq.toString());
			WordAndCount wordAndCount = pq.poll();
			context.write(wordAndCount.getWord(), wordAndCount.getTup());
		}
	}

}