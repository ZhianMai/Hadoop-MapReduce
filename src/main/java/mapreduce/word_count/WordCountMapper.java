package mapreduce.word_count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class 4 generics:
 * <KEY_IN, VALUE_IN> & <KEY_OUT, VALUE_OUT>
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
  /**
   * mapper: <KEY_IN, VALUE_IN> -> <KEY_OUT, VALUE_OUT>
   *   KEY_IN: the line number of input file
   *   VALUE_IN: the String val of line KEY_IN
   *   KEY_OUT: split word separated by ' '
   *   VALUE_OUT: 1 (constant)
   */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    // Ignore key
    Text text = new Text();
    LongWritable longWritable = new LongWritable();
    String[] splitInput = value.toString().split(" ");

    for (String world : splitInput) {
      text.set(world); // Reuse object
      longWritable.set(1);
      context.write(text, longWritable);
    }
  }
}
