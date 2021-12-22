package mapreduce.gender_partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * K1: #line offset, LongWritable
 * V1: String of #line, Text
 * K2: V1
 * V2: NullWritable as placeholder
 * */
public class PartitionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    context.write(value, NullWritable.get());
  }
}
