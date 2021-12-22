package mapreduce.word_count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer class 4 generics:
 * <KEY_IN, VALUE_IN> & <KEY_OUT, VALUE_OUT>
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

  /**
   * reducer: <KEY_IN, VALUE_IN> -> <KEY_OUT, VALUE_OUT>
   * KEY_IN: single word
   * VALUE_IN: set of 1 (constant)
   * KEY_OUT: single word
   * VALUE_OUT: count of its corresponding distinct KEY_OUT
   */
  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {

    long count = 0;
    for (LongWritable value : values) {
      count += value.get();
    }

    context.write(key, new LongWritable(count));
  }
}