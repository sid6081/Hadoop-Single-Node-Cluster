package com.impetus.HadoopImplement;

/**
 * 
 *
 */
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class WordCount 
{
	static class MapWordCount extends Mapper<Object, Text, Text, IntWritable>
	{
		IntWritable countONE=new IntWritable(1);
		Text str=new Text();
		public void map(Object ob,Text t,Context c)
		{
			StringTokenizer token=new StringTokenizer(t.toString());
			while(token.hasMoreTokens())
			{
		        try {
		        	String temp=token.nextToken();
			        for(int i=0;i<temp.length();i++)
		        	{
			        	str.set(""+temp.charAt(i));
			        	c.write(str, countONE);
		        	}

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	static class ReduceWordCount extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		IntWritable intWObj=new IntWritable();
		public void reduce(Text t,Iterable<IntWritable> itObj,Context c)
		{
			int sum=0;
		      for (IntWritable i : itObj) {
		          sum += i.get();
		        }
		    try {
			    intWObj.set(sum);
		    	c.write(t,intWObj);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
    public static void main( String[] args )
    {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "word count");
            job.setJarByClass(App.class);
            job.setMapperClass(MapWordCount.class);
            job.setReducerClass(ReduceWordCount.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
			      FileInputFormat.addInputPath(job, new Path("/user/impadmin/inputfile.txt"));
	          FileOutputFormat.setOutputPath(job, new Path("/user/impadmin/outputfile.txt"));
	          System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
