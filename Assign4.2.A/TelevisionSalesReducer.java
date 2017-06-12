package TelevisionSalesPerCompany;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author abhilasha
 * Input key is the the name of the company
 * Input value is 1
 * Output key is the company name
 * OUtput value is the count of televisions for a company
 *
 */
public class TelevisionSalesReducer extends Reducer<Text,IntWritable,Text,LongWritable>
{
	@Override
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		long lCount = 0;
		for(IntWritable value:values)
		{
			lCount=lCount + value.get();
		}
		context.write(key, new LongWritable(lCount));
	}
}
