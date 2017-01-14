package ca.uwaterloo.cs.bigdata2017w.assignment1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.lang.RuntimeException;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  // Mapper: emits (token, 1) for every word occurrence.
  public static final class OccurenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();
    private static final int CONTEXT_SIZE = 40;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      // only consider the first 40 words of each line per assignment instructions
      int loop_size = Math.min(CONTEXT_SIZE, tokens.size());

      for (int i = 0; i < loop_size; i++) {
        WORD.set(tokens.get(i));
        context.write(WORD, ONE);
      }
    }
  }

  // Reducer: sums up all the counts.
  public static final class OccurenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static final class PairsMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
    private static final FloatWritable ONE = new FloatWritable(1);
    private static final PairOfStrings PAIR = new PairOfStrings();
    private static final int CONTEXT_SIZE = 40;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      // only consider the first 40 words of each line per assignment instructions
      int loop_size = Math.min(CONTEXT_SIZE, tokens.size());

      if (tokens.size() < 2) return;
      for (int i = 0; i < loop_size; i++) {
        PAIR.set(tokens.get(i), "*");
        context.write(PAIR, ONE);        
        for(int j = i + 1; j < loop_size; j++) {
          if (!tokens.get(i).equals(tokens.get(j))) {
            PAIR.set(tokens.get(i), tokens.get(j));
            context.write(PAIR, ONE);
          }
        }

      }
    }
  }

  private static final class PairsCombiner extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      float sum = 0.0f;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static final class PairsReducer extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, PairOfFloatInt> {
    private static final FloatWritable SUM = new FloatWritable();
    private static final FloatWritable PMI = new FloatWritable();
    private static final PairOfFloatInt PMI_COUNT = new PairOfFloatInt();
    private static Map<String, Integer> word_counts = new HashMap<String, Integer>();
    private float total_words = 0;
    private float marginal = 0.0f;
    private float p_x_given_y = 0.0f;
    private float p_x = 0.0f;
    private float p_y = 0.0f;
    private float p_x_and_y = 0.0f;
    private float pmi = 0.0f;
    private int threshold = 1;

    @Override
    public void setup(Context context) throws IOException {
      threshold = context.getConfiguration().getInt("threshold", 1);

      FileSystem fs = FileSystem.get(context.getConfiguration());
      Path intermediatePath = new Path("occurrence_counts/part-r-00000");

      BufferedReader input = null;
      try{
        FSDataInputStream in = fs.open(intermediatePath);
        InputStreamReader inStream = new InputStreamReader(in);
        input = new BufferedReader(inStream);
        
      } catch(FileNotFoundException e){
        throw new IOException("Cannot open occurence counts file");
      }
      
      String line = input.readLine();
      while(line != null){
        String[] parts = line.split("\\s+");
        word_counts.put(parts[0], Integer.parseInt(parts[1]));
        total_words += Integer.parseInt(parts[1]);
        line = input.readLine();
      }
      input.close();
      LOG.info("Total words: " + total_words);

    }

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {

      int sum = 0;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      if (key.getRightElement().equals("*")) {
        marginal = sum;
      } else if (sum >= threshold) {
        // probability we will see the right word given that we have seen the left
        p_x_given_y = sum / marginal; //double check these things
        p_x_and_y = sum / total_words;
        // probability we will see the right word
        p_x = word_counts.get(key.getRightElement()) / total_words;
        p_y = word_counts.get(key.getLeftElement()) / total_words;

        pmi = (float) (Math.log(p_x_and_y / (p_x * p_y)) / Math.log(10));
        PMI_COUNT.set(pmi, sum);
        context.write(key, PMI_COUNT);
      }
    }
  }

  private static final class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
    @Override
    public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "lower bound for co-occurence")
    int threshold = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool name: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    String intermediatePath = "occurrence_counts";

    Configuration conf = getConf();
    conf.setInt("threshold", args.threshold);
    conf.set("intermediatePath", intermediatePath);
    
    Job occurenceJob = Job.getInstance(getConf());
    occurenceJob.setJobName(PairsPMI.class.getSimpleName() + "OccurenceCounts");
    occurenceJob.setJarByClass(PairsPMI.class);
    occurenceJob.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(occurenceJob, new Path(args.input));
    FileOutputFormat.setOutputPath(occurenceJob, new Path(intermediatePath));

    occurenceJob.setMapOutputKeyClass(Text.class);
    occurenceJob.setMapOutputValueClass(IntWritable.class);
    occurenceJob.setOutputKeyClass(Text.class);
    occurenceJob.setOutputValueClass(IntWritable.class);
    occurenceJob.setOutputFormatClass(TextOutputFormat.class);

    occurenceJob.setMapperClass(OccurenceMapper.class);
    occurenceJob.setCombinerClass(OccurenceReducer.class);
    occurenceJob.setReducerClass(OccurenceReducer.class);

    // Delete the output directory if it exists already.
    Path intermediateDir = new Path(intermediatePath);
    FileSystem.get(conf).delete(intermediateDir, true);

    long startTime = System.currentTimeMillis();
    occurenceJob.waitForCompletion(true);
    System.out.println("Occurence Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


    Job pairsJob = Job.getInstance(getConf());
    pairsJob.setJobName(PairsPMI.class.getSimpleName() + "PairsPMIComputation");
    pairsJob.setJarByClass(PairsPMI.class);
    pairsJob.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(pairsJob, new Path(args.input));
    FileOutputFormat.setOutputPath(pairsJob, new Path(args.output));

    pairsJob.setMapOutputKeyClass(PairOfStrings.class);
    pairsJob.setMapOutputValueClass(FloatWritable.class);
    pairsJob.setOutputKeyClass(PairOfStrings.class);
    pairsJob.setOutputValueClass(PairOfFloatInt.class);
    pairsJob.setOutputFormatClass(TextOutputFormat.class);

    pairsJob.setMapperClass(PairsMapper.class);
    pairsJob.setCombinerClass(PairsCombiner.class);
    pairsJob.setReducerClass(PairsReducer.class);
    pairsJob.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    pairsJob.waitForCompletion(true);
    System.out.println("Pairs Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}
