package mapreduce.sort_char_int;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class SortPairMain extends Configured implements Tool {
  private static boolean isLocal = false;
  private static final String INPUT_FILE_NAME = "\\input\\string_int_pair.txt";
  private static final String OUTPUT_DIR_NAME = "sorted_pair_output";
  private static final String JOB_NAME = "sort_string_int_pair";

  // The main logic of job task
  @Override
  public int run(String[] args) throws Exception {
    // Create a job task object, the configuration object should be the one
    // that passed into the ToolRunner.run()
    Job job = Job.getInstance(super.getConf(), JOB_NAME);

    // Config job
    // Basics: IO (path, format), mapper<key, val>, reducer<key, val>

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
      job.setJarByClass(SortPairMain.class); // Avoid .jar runtime error

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
    job.setMapperClass(SortPairMapper.class); 
    job.setMapOutputKeyClass(CompareCharIntPair.class);
    job.setMapOutputValueClass(NullWritable.class);

    // Setup Reducer and its output <key, val> format
    job.setReducerClass(SortPairReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // wait till the job done
    boolean bl = job.waitForCompletion(true);
    return bl ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    Configuration configuration = new Configuration();
    isLocal = true;
    // ToolRunner.run() calls the run() method above as the job task
    int run = ToolRunner.run(configuration, new SortPairMain(), args);
    System.exit(run);
  }
}
