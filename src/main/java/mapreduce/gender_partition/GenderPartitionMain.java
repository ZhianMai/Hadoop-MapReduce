package mapreduce.gender_partition;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.net.URI;

import static mapreduce.utils.SystemPathConstant.*;
import static mapreduce.utils.SystemPathConstant.HDFS_URL;

public class GenderPartitionMain extends Configured implements Tool {
  private static boolean isLocal = false;
  private static final String INPUT_FILE_NAME = "\\input\\MOCK_DATA_PERSON.csv";
  private static final String OUTPUT_DIR_NAME = "gender_partition_output";
  private static final String JOB_NAME = "gender_partition_mapreduce";
  private static final int REDUCER_AMOUNT = 2;

  @Override
  public int run(String[] strings) throws Exception {
    Job job = Job.getInstance(super.getConf(), JOB_NAME);

    Path inputPath;
    Path outputPath;

    if (isLocal) {
      inputPath = new Path(HDFS_FORMAT_MAPREDUCE_LOCAL_PATH + INPUT_FILE_NAME);
      outputPath = new Path(HDFS_FORMAT_MAPREDUCE_LOCAL_PATH + "\\" + OUTPUT_DIR_NAME);

      // Delete output file if exist, or it will throw FileAlreadyExistsException
      FileUtils.deleteDirectory(new File(JAVA_FORMAT_MAPREDUCE_LOCAL_PATH + "\\" +
          OUTPUT_DIR_NAME));
    } else {
      // For local file system, Linux use '/', and Windows use '/'
      inputPath = new Path(HDFS_URL + "/" + OUTPUT_DIR_NAME);
      outputPath = new Path(HDFS_URL + "/" + OUTPUT_DIR_NAME);
      job.setJarByClass(GenderPartitionMain.class); // Avoid .jar runtime error

      // Delete output file if exist, or it will throw FileAlreadyExistsException
      FileSystem fileSystem = FileSystem.get(new URI(HDFS_URL), new Configuration());

      if (fileSystem.exists(outputPath)) {
        // @param:        path,       isRecursivelyDelete
        fileSystem.delete(outputPath, true);
      }
    }

    TextInputFormat.addInputPath(job, inputPath);
    job.setInputFormatClass(TextInputFormat.class);
    TextOutputFormat.setOutputPath(job, outputPath);
    job.setOutputFormatClass(TextOutputFormat.class);

    // Setup Mapper and its output <key, val> format
    job.setMapperClass(PartitionMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    // Setup shuffle
    job.setPartitionerClass(GenderPartitioner.class);

    // Setup Reducer and its output <key, val> format
    job.setReducerClass(PartitionReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(REDUCER_AMOUNT);

    // wait till the job done
    System.out.println("Running");
    boolean bl = job.waitForCompletion(true);
    return bl ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    isLocal = true;
    int status = ToolRunner.run(config, new GenderPartitionMain(), args);
    System.exit(status);
  }
}
