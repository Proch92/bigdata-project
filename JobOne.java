import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Calendar;

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


// lista ordinata dei quartieri di londra per occorrenze di un determinato crimine negli ultimi 5 anni
// job1
// m: q - occ
// r: q - sum
// m: sum - q
// r: sum - q (ordered)
public class JobOne {

	public static class FilterMapper extends Mapper<Object, Text, Text, IntWritable> {

		private static IntWritable occurrencies = new IntWritable();
		private static Text neighborhood = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] record = value.toString().split(",");

			// filter records on year constraint
			int year = Integer.parseInt(record[4]);
			int currentYear = Calendar.getInstance().get(Calendar.YEAR);
			if (currentYear - year > 5) {
				return;
			}

			//filter records on crime constraint
			String crime = context.getConfiguration().get("crime");
			if (!record[2].equals(crime)) {
				return;
			}

			occurrencies.set(Integer.parseInt(record[3]));

			neighborhood.set(record[1]);
			context.write(neighborhood, occurrencies);
		}
	}

	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class InverterMapper extends Mapper<Text, Text, IntWritable, Text> {
		IntWritable valueint = new IntWritable();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			valueint.set(Integer.parseInt(value.toString()));

			context.write(valueint, key);
		}
	}

	public static class DescendingIntComparator extends WritableComparator {

		public DescendingIntComparator() {
			super(IntWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntWritable key1 = (IntWritable) w1;
			IntWritable key2 = (IntWritable) w2;          
			return -1 * key1.compareTo(key2);
		}
	}

	//params: inputfile, outputfile, crime
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("crime", args[2]);

		Job job1 = Job.getInstance(conf, "first pass");
		job1.setJarByClass(JobOne.class);
		job1.setMapperClass(FilterMapper.class);
		job1.setCombinerClass(IntSumReducer.class);
		job1.setReducerClass(IntSumReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("temp"));

		job1.waitForCompletion(true);


		Job job2 = Job.getInstance(new Configuration(), "second pass");
		job2.setJarByClass(JobOne.class);
		job2.setMapperClass(InverterMapper.class);
		job2.setSortComparatorClass(DescendingIntComparator.class);
		job2.setReducerClass(Reducer.class);
		job2.setNumReduceTasks(1);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path("temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}