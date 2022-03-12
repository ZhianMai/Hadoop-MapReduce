package mapreduce.sort_char_int;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompareCharIntPair implements WritableComparable<CompareCharIntPair> {
  private String word;
  private int num;

  public String getWord() {
    return word;
  }

  public void setWord(String word) {
    this.word = word;
  }

  public int getNum() {
    return num;
  }

  public void setNum(int num) {
    this.num = num;
  }

  @Override
  public int compareTo(CompareCharIntPair o) {
    int wordCompareVal =  word.compareTo(o.getWord());

    if (wordCompareVal == 0) {
      return Integer.compare(this.num, o.getNum());
    }

    return wordCompareVal;
  }

  // Serialize
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(word);
    dataOutput.writeInt(num);
  }

  // Deserialize
  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.word = dataInput.readUTF();
    this.num = dataInput.readInt();
  }

  @Override
  public String toString() {
    return "Pair{" +
        "word='" + word + '\'' +
        ", num=" + num +
        '}';
  }
}
