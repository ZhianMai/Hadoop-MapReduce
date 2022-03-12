package mapreduce.sort_char_int;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class 4 generics:
 * <KEY_IN, VALUE_IN> & <KEY_OUT, VALUE_OUT>
 */
public class SortPairMapper extends Mapper<LongWritable, Text, CompareCharIntPair, NullWritable> {
  /**
   * mapper: <KEY_IN, VALUE_IN> -> <KEY_OUT, VALUE_OUT>
   *   KEY_IN: the line number of input file
   *   VALUE_IN: the String val of line KEY_IN
   *   KEY_OUT: a ComparCharIntPair object
   *   VALUE_OUT: null
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] splitInput = value.toString().split(" ");
    CompareCharIntPair pair = new CompareCharIntPair();
    pair.setWord(splitInput[0]);
    pair.setNum(Integer.parseInt(splitInput[1]));
    context.write(pair, NullWritable.get());
  }
}
