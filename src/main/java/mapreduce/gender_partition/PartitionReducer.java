package mapreduce.gender_partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * K2: Text
 * V2: NullWritable
 * K3: K2
 * V3: V2
 */
public class PartitionReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
  @Override
  protected void reduce(Text key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {
    context.write(key, NullWritable.get());
  }
}
