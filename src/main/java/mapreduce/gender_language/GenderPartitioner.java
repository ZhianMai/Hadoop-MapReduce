package mapreduce.gender_language;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GenderPartitioner extends Partitioner<Text, Text> {
  /**
   * Config partition rules.
   *
   * @return partition segment number
   */
  @Override
  public int getPartition(Text key, Text val, int i) {
    // Return partition number based on gender
    if (key.toString().equals("M")) {
      return 1;
    } else {
      return 0;
    }
  }
}
