import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import WordCount.IntSumReducer;
//import WordCount.TokenizerMapper;


public class mvv {
	public static class MvvMapper 
    extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
 
 private IntWritable ix = new IntWritable();
 private IntWritable jx = new IntWritable();
   
 public void map(LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {
	
	 StringTokenizer itr = new StringTokenizer(value.toString());
	 
     while (itr.hasMoreTokens()) {
       ix.set(Integer.valueOf(itr.nextToken()));
       jx.set(Integer.valueOf(itr.nextToken()));
       context.write(jx, ix);
     }
 
 }
}
	public static class MvvReducer 
    extends Reducer<IntWritable,IntWritable,IntWritable,Text> {
 ArrayList<Integer> marray = new ArrayList<Integer>(); 
 @Override
public void setup(Context context){
	try {
		BufferedReader reader = 
				new BufferedReader(new FileReader("/home/hadoop/workspace/MVV/src/pp.txt"));
		String line = reader.readLine();
		StringTokenizer tk = new StringTokenizer(line);
		while(tk.hasMoreElements()){
			marray.add(Integer.valueOf(tk.nextToken()));
		}
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
 Text result = new Text();

 public void reduce(IntWritable key, Iterable<IntWritable> values, 
                    Context context
                    ) throws IOException, InterruptedException {
	 double x = 1.0/marray.get(key.get());
   for(IntWritable value:values){
	   result.set(value+" "+x);
	   context.write(key, result);
   }
 }
}
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = new Job(conf, "matrix");
		    job.setJarByClass(mvv.class);
		    job.setMapperClass(MvvMapper.class);
		    job.setReducerClass(MvvReducer.class);
		 //  job.setCombinerClass(IntSumReducer.class);
		  //  job.setReducerClass(IntSumReducer.class);
		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		   FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	}