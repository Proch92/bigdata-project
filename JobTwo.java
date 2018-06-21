import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Calendar;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// per ogni anno determinare i 3 quartieri con la media di crimini al giorno più alta
// record
// m: q, anno - occ
// r: q, anno - avg
// m: anno - q, avg
// r: anno - (q1, avg1), (q2, avg2), (q3, avg3)
public class JobTwo {

	public static class FilterMapper extends Mapper<Object, Text, Text, IntWritable> {

		private static IntWritable occurrencies = new IntWritable();
		private static Text compositeKey = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] record = value.toString().split(",");

			occurrencies.set(Integer.parseInt(record[3]));

			String comp = record[1] + "_" + record[4];
			compositeKey.set(comp);

			context.write(compositeKey, occurrencies);
		}
	}

	public static class AvgReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			int avg = sum / 365;

			result.set(avg);
			context.write(key, result);
		}
	}

	public static class DecupleMapper extends Mapper<Text, Text, Text, MapWritable> {
		MapWritable values = new MapWritable();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = key.split("_");

			values.put(new IntWritable(0), new Text(tokens[0]));
			values.put(new IntWritable(1), Integer.parseInt(value));

			context.write(new IntWritable(Integer.parseInt(tokens[1])), values);
		}
	}

	public static class SortReducer extends Reducer<IntWritable, MapWritable, IntWritable, MapWritable[]> {
		ArrayWritable results = new ArrayWritable(MapWritable.class);

		public void reduce(IntWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			MapWritable[] mapArray = StreamSupport.stream(values.spliterator(), false).
												sorted((o1, o2) -> o2.get(new IntWritable(1)).comapareTo(o1.get(new IntWritable(1)))).
												limit(3).
												toArray(MapWritable[]::new);

			results.set(mapArray)

			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Job job1 = Job.getInstance(new Configuration(), "first pass");
		job1.setJarByClass(JobTwo.class);
		job1.setMapperClass(FilterMapper.class);
		job1.setReducerClass(AvgReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("temp"));

		job1.waitForCompletion(true);


		Job job2 = Job.getInstance(new Configuration(), "second pass");
		job2.setJarByClass(JobTwo.class);
		job2.setMapperClass(DecupleMapper.class);
		job2.setReducerClass(Reducer.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(MapWritable.class);

		FileInputFormat.addInputPath(job2, new Path("temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}