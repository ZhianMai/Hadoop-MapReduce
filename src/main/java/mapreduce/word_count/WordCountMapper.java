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

    Text text = new Text();
    LongWritable longWritable = new LongWritable();

    // 1.将一行的文本进行拆分
    String[] split = value.toString().split(" ");
    // 2.遍历数组, 组装 K2和V2
    for (String world : split) {
      // 3.将K2和V2写入上下文
      text.set(world);
      longWritable.set(1);
      context.write(text, longWritable);
    }
  }
}
