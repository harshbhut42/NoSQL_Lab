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

 
public class Summary {
 
  public static class My_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
 
    private final static IntWritable one = new IntWritable(1);
   // private Text word = new Text();
 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String s = value.toString();
      //int l = s.indexOf("- - ["); 
      //String date = s.substring(l+8, l+16);
      if(s.contains("GET"))
      {
        String delims = "[ ]+";
        String[] tokens = s.split(delims);
        String date = tokens[3].substring(4, 12);
        
        int j = 7;
        for(int i=0; i<tokens.length; i++)
        {
            if(tokens[i].equals("HTTP/1.1" + '"'))
            {
                j = i;
            }
        }
        int data;
        if(tokens[j+2].equals("-"))
        {
            //System.out.println(tokens[0]+ " " + tokens[j-1] + " " + tokens[j] + " " + tokens[j+1]);
            data = 0;
        }
        else
        {
            data = Integer.parseInt(tokens[j+2]);
        }
        context.write(new Text(date), new IntWritable(data));
      }
      
    }
  }
 
  public static class My_Reducer extends Reducer<Text,IntWritable,Text,Text> {
    //private Text result = new Text();
 
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      long sum = 0;
      int count = 0;
      for (IntWritable val : values) {
        sum += val.get();
        count++;
      }
      //result.set((count + ", "+ sum));
      context.write(key, new Text(count + ", " + sum));
    }
  }
 
  public static void main(String[] args) throws Exception {
	
	Configuration conf = new Configuration();
	
	Job job = Job.getInstance(conf, "Summary");
    job.setJarByClass(Summary.class);
	
	job.setMapperClass(My_Mapper.class);
    job.setReducerClass(My_Reducer.class);
	
	job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
