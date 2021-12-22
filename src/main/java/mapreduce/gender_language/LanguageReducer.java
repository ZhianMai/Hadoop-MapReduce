package mapreduce.gender_language;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * K2: Text
 * V2: NullWritable
 * K3: K2
 * V3: V2
 */
public class LanguageReducer extends Reducer<Text, Text, Text, LongWritable> {
  public static enum GenderCounter {
    MALE,
    FEMALE
  }

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    int reducerId = context.getTaskAttemptID().getTaskID().getId();
    Counter genderCounter;

    if (reducerId == 0) {
      genderCounter = context.getCounter(GenderCounter.FEMALE);
    } else {
      genderCounter = context.getCounter(GenderCounter.MALE);
    }

    Map<String, Long> languageCounter = new HashMap<>();

    for (Text text : values) {
      genderCounter.increment(1L);
      String language = text.toString();
      Long speakerAmount = languageCounter.get(language);

      if (speakerAmount == null) {
        languageCounter.put(language, 1L);
      } else {
        languageCounter.put(language, speakerAmount + 1L);
      }
    }

    LongWritable val = new LongWritable();
    for (Map.Entry<String, Long> entry : languageCounter.entrySet()) {
      key.set(entry.getKey());
      val.set(entry.getValue());
      context.write(key, val);
    }
  }
}
