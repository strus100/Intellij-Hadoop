import java.io.IOException;

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

public class MapReduceMain
{
	public static class CustomMapper extends Mapper< Object, Text, Text, IntWritable >
	{
		private final Text word = new Text();

		public void map( Object key,Text value,Context context ) throws IOException, InterruptedException
		{
			if( ( (LongWritable) key ).get() != 0 )
			{
				String[] line = value.toString().split( "," );

				word.set( line[ 1 ] );
				int rate = Integer.parseInt( line[ 3 ] );

				context.write( word,new IntWritable( rate ) );
			}
		}
	}

	public static class CustomReducer extends Reducer< Text, IntWritable, Text, CustomOutputValue >
	{
		private CustomOutputValue result = new CustomOutputValue();

		public void reduce( Text filmId,Iterable< IntWritable > rates,Context context ) throws IOException,
																									   InterruptedException
		{
			int sum = 0;
			int count = 0;
			for( IntWritable val : rates )
			{
				sum += val.get();
				count++;
			}
			result.setSum( sum );
			result.setCount( count );
			context.write( filmId,result );
		}
	}

	public static void main( String[] args ) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance( conf,"Step one map reduce" );
		job.setJarByClass( MapReduceMain.class );
		job.setMapperClass( CustomMapper.class );
		job.setReducerClass( CustomReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setMapOutputValueClass( IntWritable.class );
		job.setOutputValueClass( CustomOutputValue.class );
		FileInputFormat.addInputPath( job,new Path( args[ 0 ] ) );
		FileOutputFormat.setOutputPath( job,new Path( args[ 1 ] ) );
		System.exit( job.waitForCompletion( true ) ? 0 : 1 );
	}
}
