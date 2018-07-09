import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Calendar;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.stream.StreamSupport;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
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

			occurrencies.set(Integer.parseInt(record[4]));

			String comp = record[1] + "_" + record[5];
			compositeKey.set(comp);

			context.write(compositeKey, occurrencies);
		}
	}

	public static class AvgReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			float sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			float avg = sum / 365;

			context.write(key, new FloatWritable(avg));
		}
	}

	public static class DecupleMapper extends Mapper<Object, Text, Text, MapWritable> {
		MapWritable values = new MapWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			String[] keys = tokens[0].split("_");
			float avg = Float.parseFloat(tokens[1]);

			values.put(new IntWritable(0), new Text(keys[0]));
			values.put(new IntWritable(1), new FloatWritable(avg));

			context.write(new Text(keys[1]), values);
		}
	}

	public static class SortReducer extends Reducer<Text, MapWritable, Text, Text> {
		private String mapToString (MapWritable m) {
			return new String("(" + m.get(new IntWritable(0)).toString() + ", " + m.get(new IntWritable(1)).toString() + ")");
		}

		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			String results = StreamSupport.stream(values.spliterator(), false).
												sorted((o1, o2) -> ((FloatWritable) o2.get(new IntWritable(1))).compareTo(((FloatWritable) o1.get(new IntWritable(1))))).
												limit(3).
												map(m -> mapToString(m)).
												collect(Collectors.joining(" "));

			context.write(key, new Text(results));
		}
	}

	public static void main(String[] args) throws Exception {
		Job job1 = Job.getInstance(new Configuration(), "first pass");
		job1.setJarByClass(JobTwo.class);
		job1.setMapperClass(FilterMapper.class);
		job1.setReducerClass(AvgReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("temp2"));

		job1.waitForCompletion(true);


		Job job2 = Job.getInstance(new Configuration(), "second pass");
		job2.setJarByClass(JobTwo.class);
		job2.setMapperClass(DecupleMapper.class);
		job2.setReducerClass(SortReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(MapWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path("temp2"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
