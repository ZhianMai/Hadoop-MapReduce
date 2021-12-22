package mapreduce.gender_language;

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
public class GenderLanguageMapper extends Mapper<LongWritable, Text, Text, Text> {
  private static final int GENDER_COLUMN = 4;
  private static final int LANGUAGE_COLUMN = 6;

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] split = value.toString().split(",");
    Text keyText = new Text(split[GENDER_COLUMN]);
    Text valText = new Text(split[LANGUAGE_COLUMN]);
    context.write(keyText, valText);
  }
}
