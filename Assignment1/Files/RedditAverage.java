import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer ;

import org.apache.hadoop.io.DoubleWritable;

import org.json.JSONObject;

public class RedditAverage  extends Configured implements Tool {
    public static class JsonMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private LongPairWritable pair = new LongPairWritable();
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
            JSONObject record = new JSONObject(value.toString());

            word.set((String) record.get("subreddit"));
            //score, num
            pair.set((Integer) record.get("score"),1);
			context.write(word, pair);
			
		}
    }
    
    public static class LongPairSumCombiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		private LongPairWritable result = new LongPairWritable(0,0);

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
            Long sumScore = 0L;
            Long sumNum =0L;
			for (LongPairWritable val : values) {
                sumScore += val.get_0();
                sumNum += val.get_1();
            }
           if (sumNum!=0) 
           {
			    result.set(sumScore,sumNum);
                context.write(key, result);
           }
           else
           System.out.println ("sumNum is zero in combiner");
		}
    }
    
	public static class LongAvgReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
            Long sumScore = (Long) 0L;
            Long sumNum = (Long) 0L;
			for (LongPairWritable val : values) {
                sumScore += val.get_0();
                sumNum += val.get_1();
            }
            
			result.set((sumNum!=0?(double)sumScore/sumNum:0));
			context.write(key, result);
		}
	}


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "score avg");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(JsonMapper.class);
		job.setCombinerClass(LongPairSumCombiner.class);
		job.setReducerClass(LongAvgReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongPairWritable.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
        
        TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}