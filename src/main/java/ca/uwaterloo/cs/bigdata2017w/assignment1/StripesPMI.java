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
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HashMapWritable;
import org.apache.hadoop.io.MapWritable;

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

public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  // Mapper: emits (token, 1) for every word occurrence.
  public static final class OccurenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text WORD = new Text();
    private static final int CONTEXT_SIZE = 40;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
      
      Set<String> distinct = new HashSet<String>();
      // only consider the first 40 words of each line per assignment instructions
      int loop_size = Math.min(CONTEXT_SIZE, tokens.size());

      WORD.set("*");
      context.write(WORD, ONE);

      for (int i = 0; i < loop_size; i++) {
        if (distinct.add(tokens.get(i))) {
          WORD.set(tokens.get(i));
          context.write(WORD, ONE);
        }
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

  private static final class StripesMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final HMapStIW MAP = new HMapStIW();  
    private static final Text KEY = new Text();
    private static final int CONTEXT_SIZE = 40;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      // only consider the first 40 words of each line per assignment instructions
      int loop_size = Math.min(CONTEXT_SIZE, tokens.size());

      if (tokens.size() < 2) return;
      for (int i = 0; i < loop_size; i++) {
        for (int j = i + 1; j < loop_size; j++) {
          if (!tokens.get(i).equals(tokens.get(j)) && !MAP.containsKey(tokens.get(j))) {
              MAP.put(tokens.get(j), 1);
          }
        }
        KEY.set(tokens.get(i));
        context.write(KEY, MAP);
        MAP.clear();
      }
    }
  }

  private static final class StripesCombiner extends
      Reducer<Text, HMapStIW, Text, HMapStIW> {
    private static final HMapStIW COMBINED = new HMapStIW();

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {

      for (HMapStIW map : values) {
        for (String word : map.keySet()) {
          if (COMBINED.containsKey(word)) {
            COMBINED.put(word, COMBINED.get(word) + map.get(word));
          } else {
            COMBINED.put(word, map.get(word));
          }
        }
      }

      context.write(key, COMBINED);
      COMBINED.clear();
    }
  }

  private static final class StripesReducer extends
      Reducer<Text, HMapStIW, Text, HashMapWritable> {
    private static final Map<String, Integer> COMBINED = new HashMap<String, Integer>();
    private static final HMapStIW MAP = new HMapStIW();
    private static final HashMapWritable PMI_MAP = new HashMapWritable();
    private static final FloatWritable SUM = new FloatWritable();
    private static final Text PMI_KEY = new Text();
    private static final PairOfFloatInt PMI_COUNT = new PairOfFloatInt();
    private static Map<String, Integer> word_counts = new HashMap<String, Integer>();
    private float total_lines = 0.0f;
    private float p_x = 0.0f;
    private float p_y = 0.0f;
    private float p_x_and_y = 0.0f;
    private float pmi = 0.0f;
    private int threshold = 1;

    @Override
    public void setup(Context context) throws IOException {
      threshold = context.getConfiguration().getInt("threshold", 1);

      FileSystem fs = FileSystem.get(context.getConfiguration());
      Path intermediatePath = new Path("lmdineen_occurrence_counts_stripes/part-r-00000");

      BufferedReader input = null;
      try {
        FSDataInputStream in = fs.open(intermediatePath);
        InputStreamReader inStream = new InputStreamReader(in);
        input = new BufferedReader(inStream);
        
      } catch(FileNotFoundException e) {
        throw new IOException("Cannot open occurence counts file");
      }
      
      String line = input.readLine();
      while (line != null) {
        String[] parts = line.split("\\s+");
        if (parts[0].equals("*")){
          total_lines = Integer.parseInt(parts[1]);
        } else {
          word_counts.put(parts[0], Integer.parseInt(parts[1]));
        }
        line = input.readLine();
      }
      input.close();
      LOG.info("Total lines: " + total_lines);
    }

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {

        //try making combined a regular hashmap and then make it writable later
          // like after the pmi calculation
       for (HMapStIW map : values) {
        for (String word : map.keySet()) {
          if (COMBINED.containsKey(word)) {
            COMBINED.put(word, COMBINED.get(word) + map.get(word));
          } else {
            COMBINED.put(word, map.get(word));
          }
        }
      }

      Iterator<String> it = COMBINED.keySet().iterator();
      while(it.hasNext()) {
        String word = it.next();
        int count = COMBINED.get(word);
        if (count < threshold) {
            it.remove();
        } else {
          p_x_and_y = count / total_lines;
          // probability we will see the right word
          p_y = word_counts.get(word) / total_lines;
          p_x = word_counts.get(key.toString()) / total_lines;

          pmi = (float) (Math.log(p_x_and_y / (p_x * p_y)) / Math.log(10));
          PairOfFloatInt pmi_count = new PairOfFloatInt(pmi, count);
          Text pmi_key = new Text(word);
          PMI_MAP.put(pmi_key, pmi_count);
        }
      }
      
      if (PMI_MAP.size() > 0) {
        context.write(key, PMI_MAP);
      }
      COMBINED.clear();
      PMI_MAP.clear();
    }
  }

  private static final class MyPartitioner extends Partitioner<Text, HMapStIW> {
    @Override
    public int getPartition(Text key, HMapStIW value, int numReduceTasks) {
      return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

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

    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    String intermediatePath = "lmdineen_occurrence_counts_stripes";

    Configuration conf = getConf();
    conf.setInt("threshold", args.threshold);
    conf.set("intermediatePath", intermediatePath);
    
    Job occurenceJob = Job.getInstance(getConf());
    occurenceJob.setJobName(StripesPMI.class.getSimpleName() + "OccurenceCounts");
    occurenceJob.setJarByClass(StripesPMI.class);
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


    Job stripesJob = Job.getInstance(getConf());
    stripesJob.setJobName(StripesPMI.class.getSimpleName() + "Computation");
    stripesJob.setJarByClass(StripesPMI.class);
    stripesJob.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(stripesJob, new Path(args.input));
    FileOutputFormat.setOutputPath(stripesJob, new Path(args.output));

    stripesJob.setMapOutputKeyClass(Text.class);
    stripesJob.setMapOutputValueClass(HMapStIW.class);
    stripesJob.setOutputKeyClass(Text.class);
    stripesJob.setOutputValueClass(HashMapWritable.class);
    stripesJob.setOutputFormatClass(TextOutputFormat.class);

    stripesJob.setMapperClass(StripesMapper.class);
    stripesJob.setCombinerClass(StripesCombiner.class);
    stripesJob.setReducerClass(StripesReducer.class);
    stripesJob.setPartitionerClass(MyPartitioner.class);

    stripesJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    stripesJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    stripesJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    stripesJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    stripesJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    stripesJob.waitForCompletion(true);
    System.out.println("Stripes Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
