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
import org.apache.hadoop.io.SequenceFile;
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
import java.lang.InstantiationException;
import java.lang.IllegalAccessException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  public ExtractTopPersonalizedPageRankNodes() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  private void extractTopNodes(String inputPath, int numNodes, String sourceNodes) throws IOException, InstantiationException, IllegalAccessException {
    ArrayList<String> sources = new ArrayList(Arrays.asList(sourceNodes.split(",")));
    ArrayList<TopScoredObjects<Integer>> queue = new ArrayList<TopScoredObjects<Integer>>();
    for (int s = 0; s < sources.size(); s++) {
      queue.add(new TopScoredObjects<Integer>(numNodes));
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    // find all the part-m files
    int i = 0;
    Path p = new Path(String.format(inputPath + "/part-m-%05d", i));
    while(fs.exists(p)){
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
      IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
      PageRankNode value = (PageRankNode) reader.getValueClass().newInstance();

      while(reader.next(key, value)){
        for(int s = 0; s < sources.size(); s++){
          queue.get(s).add(key.get(), value.getPageRank(s));
        }
      }
      reader.close();

      i++;
      p = new Path(String.format(inputPath + "/part-m-%05d", i));
    }


    for(int s = 0; s < sources.size(); s++){
      System.out.println("Source: " + sources.get(s));

      for (PairOfObjectFloat<Integer> pair : queue.get(s).extractAll()) {
        System.out.println(String.format("%.5f %d", Math.exp(pair.getRightElement()), pair.getLeftElement()));
      }

      System.out.println();  
    }

  }

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

    extractTopNodes(inputPath, n, sources);

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
