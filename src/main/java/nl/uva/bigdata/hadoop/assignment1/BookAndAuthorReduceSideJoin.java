package nl.uva.bigdata.hadoop.assignment1;

import nl.uva.bigdata.hadoop.HadoopJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;


public class BookAndAuthorReduceSideJoin extends HadoopJob {

  @Override
  public int run(boolean onCluster, JobConf jobConf, String[] args) throws Exception {

    Map<String, String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job job;

    if (onCluster) {
      job = new Job(jobConf);
    } else {
      job = new Job();
    }

    Configuration conf = job.getConfiguration();

    job.setReducerClass(BookAndAuthorReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job, authors, TextInputFormat.class, AuthorMapper.class);
    MultipleInputs.addInputPath(job, books, TextInputFormat.class, BookMapper.class);

    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);

    return 0;
  }

  public static class AuthorMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

      String[] input = value.toString().split("\t");

      Text id = new Text(input[0]);
      Text author = new Text(input[1]);

      context.write(id, author);
    }
  }

  public static class BookMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

      String[] input = value.toString().split("\t");

      Text id = new Text(input[0]);
      Text book_info = new Text(input[2] + "\t" + input[1]);

      context.write(id, book_info);
    }
  }

  public static class BookAndAuthorReducer extends Reducer<Text, Text, Text, NullWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

      ArrayList<String> book_info = new ArrayList<String>();
      String author = "";

      for (Text val : values) {
        //
        if (val.toString().indexOf('\t') == -1) {
          author = val.toString();
        }
        else {
          book_info.add(val.toString());
        }
      }
      if (!author.equals("")) {
        for (String book : book_info) {
          Text output = new Text(author + "\t" + book);
          context.write(output, NullWritable.get());
        }
      }
    }
  }
}