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

	// fa il primo filtraggio sui record in base all'anno e al crimine commesso
	public static class FilterMapper extends Mapper<Object, Text, Text, IntWritable> {

		private static IntWritable occurrencies = new IntWritable();
		private static Text neighborhood = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] record = value.toString().split(",");

			int year = Integer.parseInt(record[5]);

			Configuration conf = context.getConfiguration();
			String crime = conf.get("crime");

			if (year >= 2013 && crime.equals(record[2])) {
				occurrencies.set(Integer.parseInt(record[4]));
			} else {
				occurrencies.set(0);
			}

			neighborhood.set(record[1]);
			context.write(neighborhood, occurrencies);
		}
	}

	// aggregazione sulla somma
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			System.out.println(key + " " + sum);
			result.set(sum);
			context.write(key, result);
		}
	}

	// scambia chiave e valore per poter ordinare sulla chiave con il reducer
	public static class InverterMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			String neighborhood = tokens[0];
			int sum = Integer.parseInt(tokens[1]);

			context.write(new IntWritable(sum), new Text(neighborhood));
		}
	}

	// comparatore custom per ordinare in modo decrescente
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
		conf.set("crime", "Robbery");

		Job job1 = Job.getInstance(conf, "first pass");
		job1.setJarByClass(JobOne.class);
		job1.setMapperClass(FilterMapper.class);
		job1.setCombinerClass(IntSumReducer.class);		// posso usare il mio reducer come combiner per ottimizzare l'I/O
		job1.setReducerClass(IntSumReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("temp1"));

		job1.waitForCompletion(true);


		Job job2 = Job.getInstance(new Configuration(), "second pass");
		job2.setJarByClass(JobOne.class);
		job2.setMapperClass(InverterMapper.class);
		job2.setSortComparatorClass(DescendingIntComparator.class);
		job2.setReducerClass(Reducer.class);							// uso la classe base Reducer e forzo
		job2.setNumReduceTasks(1);										// 1 task per ottenere un ordinamento sulla chiave
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path("temp1"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
