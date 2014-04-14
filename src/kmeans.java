import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class kmeans extends Configured implements Tool{
    private static final Log log = LogFactory.getLog(kmeans.class);
    
    private static final int K = 10;
    private static final int MAXITERATIONS = 20;
    private static final double THRESHOLD = 0.01;
    public static boolean stopIteration(Configuration conf) throws IOException{
    	FileSystem fs = FileSystem.get(conf);
    	Path prevCenterFile = new Path("/user/hadoop/input/centers");
    	Path currCenterFile = new Path("/user/hadoop/output/part-r-00000");
    	if(!(fs.exists(prevCenterFile) && fs.exists(currCenterFile))){
            log.info("file not exsit");
            System.exit(1);
        }
    	boolean stop = true;
    	String line1,line2;
    	FSDataInputStream in1 = fs.open(prevCenterFile);
    	FSDataInputStream in2 = fs.open(currCenterFile);
    	BufferedReader br1 = new BufferedReader(new InputStreamReader(in1));
    	BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));
    	Samples prevCenter,currCenter;
    	while((line1=br1.readLine())!=null&&(line2=br2.readLine())!=null){
    		prevCenter=new Samples();
    		currCenter=new Samples();
    		String []str1=line1.split("\\s+");
    		String []str2=line2.split("\\s+");
    		assert(str1[0].equals(str2[0]));
            for(int i=1;i<=Samples.getDIM();i++){
            prevCenter.getarr()[i-1]=Double.parseDouble(str1[i]);
            currCenter.getarr()[i-1]=Double.parseDouble(str2[i]);
        }
        if(Samples.getEulerDist(prevCenter, currCenter)>THRESHOLD){
            stop=false;
            break;
        }
    	}
        if(stop==false){
            fs.delete(prevCenterFile,true);
            if(fs.rename(currCenterFile, prevCenterFile)==false){
                log.error("rename failed");
                System.exit(1);
            }
        }
        return stop;
    }
    public static class ClusterMapper extends Mapper<LongWritable,Text,IntWritable,Samples>{
    	Vector<Samples> centers= new Vector<Samples>();
		private BufferedReader br;
    	@Override
    	public void setup(Context context){
    		for(int i=0;i<K;i++){
    			centers.add(new Samples());
    		}
    	}
    	@Override
    	public void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
    		String []str=value.toString().split("\\s+");
    		if(str.length!=Samples.getDIM()+1){
    			log.error("dimention error");
    			System.exit(1);
    		}
    		int index = Integer.parseInt(str[0]);
    		for(int i=1;i<str.length;i++)
    			centers.get(index).getarr()[i-1]=Double.parseDouble(str[i]);
    		
    	}
    	@Override
    	public void cleanup(Context context) throws IOException,InterruptedException{
    		Path []caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
    		if(caches==null||caches.length<=0)
    		{
    			log.error("data error");
    			System.exit(1);
    		}
    		br = new BufferedReader(new FileReader(caches[0].toString()));
    		Samples sample;
    		String line;
    		while((line=br.readLine())!=null)
    		{
    			sample = new Samples();
    			String []str=line.split("\\s+");
    			for(int i=0;i<Samples.getDIM();i++)
    			{
    				sample.getarr()[i]=Double.parseDouble(str[i]);
    			}
    			int index = -1;
    			double minDist = Double.MAX_VALUE;
    			double dist;
    			for(int i=0;i<K;i++){
    				dist = Samples.getEulerDist(sample, centers.get(i));
    				if(dist<minDist){
    					minDist = dist;
    					index = i;
    				}
    			}
    			context.write(new IntWritable(index), sample);
    		}
    	}
    }
    public static class UpdateCenterReducer extends Reducer<IntWritable,Samples,IntWritable,Samples>{
    	int prev = -1;
    	Samples center = new Samples();
    	int count =0;
    	@Override
    	public void reduce(IntWritable key,Iterable<Samples> values,Context context) throws IOException,InterruptedException{
    		while(values.iterator().hasNext()){
    			Samples value = values.iterator().next();
    			if(key.get()!=prev){
    				if(prev!=-1){
    					for(int i=0;i<center.getarr().length;i++)
    						center.getarr()[i]/=count;
    					context.write(new IntWritable(prev),center);
    				}
    				prev=key.get();
    				center.clear();
    				count = 0;
    			}
    			for(int i=0;i<Samples.getDIM();i++)
    				center.getarr()[i]+=value.getarr()[i];
    			count++;
    		}
    	}
    	@Override
    	public void cleanup(Context context)throws IOException,InterruptedException{
    		for(int i=0;i<center.getarr().length;i++)
    			center.getarr()[i]/=count;
    		context.write(new IntWritable(prev), center);
    	}
    }
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf);
		job.setJarByClass(kmeans.class);
		FileInputFormat.setInputPaths(job, "/user/hadoop/input/centers");
		Path outDir = new Path("/user/hadoop/output");
		fs.delete(outDir,true);
		FileOutputFormat.setOutputPath(job, outDir);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(ClusterMapper.class);
        job.setReducerClass(UpdateCenterReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Samples.class);
        return job.waitForCompletion(true)?0:1;
	}
	public static void main(String []args)throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path datafile = new Path("/user/hadoop/input/data");
		DistributedCache.addCacheFile(datafile.toUri(), conf);
		
		int iteration = 0;
		int success = 1;
		do{
			success^= ToolRunner.run(conf, new kmeans(), args);
			log.info("iteration "+iteration+" end");
		}while(success ==1 && iteration++ < MAXITERATIONS && (!stopIteration(conf)));
		log.info("Success.Iteration=" + iteration);
		//
		Job job = new Job(conf);
		job.setJarByClass(kmeans.class);
		
		FileInputFormat.setInputPaths(job,"/user/hadoop/input/centers");
		Path outDir = new Path("/user/hadoop/output2");
		fs.delete(outDir,true);
		FileOutputFormat.setOutputPath(job, outDir);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(ClusterMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Samples.class);
        
        job.waitForCompletion(true);
	}

}
