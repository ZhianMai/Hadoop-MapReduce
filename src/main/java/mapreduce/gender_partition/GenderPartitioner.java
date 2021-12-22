package mapreduce.gender_partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class GenderPartitioner extends Partitioner<Text, NullWritable> {
  /**
   * Config partition rules.
   *
   * @return partition segment number
   */
  @Override
  public int getPartition(Text text, NullWritable nullWritable, int i) {
    // Split text to get the gender column
    String[] split = text.toString().split(",");

    // Return partition number based on gender
    if (split[4].equals("M")) {
      return 1;
    } else {
      return 0;
    }
  }
}
