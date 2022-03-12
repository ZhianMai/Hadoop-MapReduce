package mapreduce.sort_char_int;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer class 4 generics:
 * <KEY_IN, VALUE_IN> & <KEY_OUT, VALUE_OUT>
 */
public class SortPairReducer extends Reducer<CompareCharIntPair, NullWritable,
                                             Text, LongWritable> {

  /**
   * reducer: <KEY_IN, VALUE_IN> -> <KEY_OUT, VALUE_OUT>
   * KEY_IN: single word
   * VALUE_IN: set of 1 (constant)
   * KEY_OUT: single word
   * VALUE_OUT: count of its corresponding distinct KEY_OUT
   */
  @Override
  protected void reduce(CompareCharIntPair key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {
    Text text = new Text(key.getWord());
    LongWritable num = new LongWritable(key.getNum());
    context.write(text, num);
  }
}