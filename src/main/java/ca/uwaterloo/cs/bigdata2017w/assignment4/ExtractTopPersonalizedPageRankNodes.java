/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs.bigdata2017w.assignment4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static class MyMapper extends
      Mapper<IntWritable, PageRankNode, Text, PairOfObjectFloat<Integer>> {
    private ArrayList<TopScoredObjects<Integer>> queue;
    private static ArrayList<String> sources;

    @Override
    public void setup(Context context) throws IOException {
      String[] srcs = context.getConfiguration().getStrings("sources");
      sources = new ArrayList(Arrays.asList(srcs));
      
      int k = context.getConfiguration().getInt("n", 100);
      queue = new ArrayList<TopScoredObjects<Integer>>();
      
      for (int s = 0; s < sources.size(); s++) {
        queue.add(new TopScoredObjects<Integer>(k));
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {
      for(int s = 0; s < sources.size(); s++){
        queue.get(s).add(node.getNodeId(), node.getPageRank(s));
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      Text src = new Text();
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();
      for(int s = 0; s < sources.size(); s++){

        for (PairOfObjectFloat<Integer> pair : queue.get(s).extractAll()) {
          // key.set(pair.getLeftElement());
          // value.set(pair.getRightElement());
          src.set(sources.get(s));
          context.write(src, pair);
        }
      }

      // Text sourceString = new Text("Source:");
      // Text sourceNode = new Text();
      // Text pagerank = new Text();
      // Text nodeId = new Text();
      // float pr = 0.0f;
      // int id = 0;

      // for(int s = 0; s < sources.size(); s++){
      //   sourceNode.set(sources.get(s));
      //   System.out.println("Source: " + sources.get(s));
      //   context.write(sourceString, sourceNode);

      //   for (PairOfObjectFloat<Integer> pair : queue.get(s).extractAll()) {
      //     pr = pair.getRightElement();
      //     id = pair.getLeftElement();
      //     nodeId.set(pair.getLeftElement().toString());
      //     // We're outputting a string so we can control the formatting.
      //     pagerank.set(String.format("%.5f", pair.getRightElement()));
          
      //     System.out.println(String.format("%.5f %d", pr, id));

      //     context.write(pagerank, nodeId);
      //   }

      //   System.out.println();  
      // }
    }
  }

  private static class MyReducer extends
      Reducer<Text, PairOfObjectFloat<Integer>, Text, Text> {
    private static ArrayList<TopScoredObjects<Integer>> queue;
    private static ArrayList<String> sources;

    @Override
    public void setup(Context context) throws IOException {
      String[] srcs = context.getConfiguration().getStrings("sources");
      sources = new ArrayList(Arrays.asList(srcs));
      
      int k = context.getConfiguration().getInt("n", 100);
      queue = new ArrayList<TopScoredObjects<Integer>>();
      
      for (int s = 0; s < sources.size(); s++) {
        queue.add(new TopScoredObjects<Integer>(k));
      }
    }

    @Override
    public void reduce(Text src, Iterable<PairOfObjectFloat<Integer>> iterable, Context context)
        throws IOException {
      Iterator<PairOfObjectFloat<Integer>> iter = iterable.iterator();
      int srcIndex = sources.indexOf(src.toString());

      while (iter.hasNext()){
        PairOfObjectFloat<Integer> pair = iter.next();
        queue.get(srcIndex).add(pair.getLeftElement(), pair.getRightElement());
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      // IntWritable key = new IntWritable();
      // Text value = new Text();

      // for (PairOfObjectFloat<Integer> pair : queue.extractAll()) {
      //   key.set(pair.getLeftElement());
      //   // We're outputting a string so we can control the formatting.
      //   value.set(String.format("%.5f", pair.getRightElement()));
      //   context.write(key, value);
      // }


      Text sourceString = new Text("Source:");
      Text sourceNode = new Text();
      Text pagerank = new Text();
      Text nodeId = new Text();
      float pr = 0.0f;
      int id = 0;

      for(int s = 0; s < sources.size(); s++){
        sourceNode.set(sources.get(s));
        System.out.println("Source: " + sources.get(s));
        context.write(sourceString, sourceNode);

        for (PairOfObjectFloat<Integer> pair : queue.get(s).extractAll()) {
          pr = pair.getRightElement();
          id = pair.getLeftElement();
          nodeId.set(pair.getLeftElement().toString());
          // We're outputting a string so we can control the formatting.
          pagerank.set(String.format("%.5f", pair.getRightElement()));
          
          System.out.println(String.format("%.5f %d", pr, id));

          context.write(pagerank, nodeId);
        }

        System.out.println();  
      }
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";


  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("string").hasArg()
        .withDescription("list of sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sources = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - sources: " + sources);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    conf.setStrings("sources", sources);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfObjectFloat.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}
