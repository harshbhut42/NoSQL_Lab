import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

 
public class TimestampURL {
 
  public static class My_Mapper extends Mapper<LongWritable, Text, Text, Text>{
 
    private final static IntWritable one = new IntWritable(1);
   // private Text word = new Text();
 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String s = value.toString();
      //int l = s.indexOf("- - ["); 
      //String date = s.substring(l+8, l+16);
      if(s.contains("404"))
      {
        String delims = "[ ]+";
        String[] tokens = s.split(delims);
        String time = tokens[3].substring(1);
        String url = tokens[10];
        
        context.write(new Text(time), new Text(url));
      } 
    }
  }
 
  public static void main(String[] args) throws Exception {
	
	Configuration conf = new Configuration();
	
	Job job = Job.getInstance(conf, "TimestampURL");
    job.setJarByClass(TimestampURL.class);
	
	job.setMapperClass(My_Mapper.class);
  //  job.setReducerClass(My_Reducer.class);
	
	job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
