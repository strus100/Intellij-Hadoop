import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
	public static class CustomMapper extends Mapper< Object, Text, Text, SumCount >
	{
		private SumCount sumCount = new SumCount();
		private final Text word = new Text();

		public void map( Object key,Text value,Context context ) throws IOException, InterruptedException
		{
			if( ( (LongWritable) key ).get() != 0 )
			{
				String[] line = value.toString().split( "," );

				word.set( line[ 1 ] );
				int rate = Integer.parseInt( line[ 3 ] );
				sumCount.set( new DoubleWritable( rate ), new IntWritable( 1 ) );

				context.write( word, sumCount );
			}
		}
	}

	public static class CustomReducer extends Reducer< Text, SumCount, Text, CustomOutputValue >
	{
		private CustomOutputValue result = new CustomOutputValue();
		SumCount sumCount = new SumCount();

		public void reduce( Text filmId, Iterable< SumCount > rates, Context context ) throws IOException, InterruptedException
		{
			int sum = 0;
			int count = 0;
			for( SumCount val : rates )
			{
//				sumCount.addSumCount(val);
				sum += val.getSum().get();
				count += val.getCount().get();
			}

			result.setSum( sum );
			result.setCount( count );
			context.write( filmId,result );
		}
	}

	public static class CustomCombiner extends Reducer< Text, SumCount, Text, SumCount >
	{
		SumCount sum = new SumCount();
		Text oldFilmId;
		public void reduce( Text filmId, Iterable< SumCount > rates, Context context ) throws IOException, InterruptedException
		{
			if( !filmId.equals(oldFilmId) )
			{
				sum = new SumCount();
			}

			for( SumCount custom: rates )
			{
				sum.addSumCount(custom);
			}
			context.write( filmId, sum );
		}
	}

	public static void main( String[] args ) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance( conf,"Step one map reduce" );
		job.setJarByClass( MapReduceMain.class );
		job.setMapperClass( CustomMapper.class );
		job.setCombinerClass( CustomCombiner.class );
		job.setReducerClass( CustomReducer.class );
		job.setOutputKeyClass( Text.class );
		job.setMapOutputValueClass( SumCount.class );
		job.setOutputValueClass( CustomOutputValue.class );
		FileInputFormat.addInputPath( job,new Path( args[ 0 ] ) );
		FileOutputFormat.setOutputPath( job,new Path( args[ 1 ] ) );
		System.exit( job.waitForCompletion( true ) ? 0 : 1 );
	}
}
