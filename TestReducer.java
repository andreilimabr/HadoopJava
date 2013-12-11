package br.com.usd.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TestReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text _key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		Text key = _key;
		int frequencyForYear = 0;
		while (values.hasNext()) {
			IntWritable value = (IntWritable) values.next();
			frequencyForYear += value.get();
		}
		output.collect(key, new IntWritable(frequencyForYear));
	}

}
