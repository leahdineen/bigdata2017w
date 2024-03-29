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

package ca.uwaterloo.cs.bigdata2017w.assignment3;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.pair.PairOfWritables;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.VIntWritable;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
    private static final PairOfStringInt KEY_PAIR = new PairOfStringInt();
    private static final IntWritable TERM_FREQ = new IntWritable();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        KEY_PAIR.set(e.getLeftElement(), (int) docno.get());
        TERM_FREQ.set(e.getRightElement());
        context.write(KEY_PAIR, TERM_FREQ);
      }
    }
  }

  private static final class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {
    
    private static final VIntWritable NUM_POSTINGS = new VIntWritable();
    private static final IntWritable ID_PREV = new IntWritable();
    private static final Text TERM = new Text();

    private final static ByteArrayOutputStream POSTING_BYTE_STREAM = new ByteArrayOutputStream();
    private final static DataOutputStream POSTING_OUT_STREAM = new DataOutputStream(POSTING_BYTE_STREAM);

    @Override
    public void setup(Context context) {
        NUM_POSTINGS.set(0);
        ID_PREV.set(0);
        TERM.set("");
    }

    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      
      Iterator<IntWritable> iter = values.iterator();

      // Emit posting when we see a new term
      if(!TERM.toString().equals(key.getLeftElement()) && !TERM.toString().equals("")){
        // write to destination
        POSTING_OUT_STREAM.flush();
        POSTING_BYTE_STREAM.flush();

        ByteArrayOutputStream contextByteStream = new ByteArrayOutputStream(4 + POSTING_BYTE_STREAM.size());
        DataOutputStream contextOutStream = new DataOutputStream(contextByteStream);
        // output doc frequency first
        WritableUtils.writeVInt(contextOutStream, NUM_POSTINGS.get());
        // output posting list
        contextOutStream.write(POSTING_BYTE_STREAM.toByteArray());
        context.write(TERM, new BytesWritable(contextByteStream.toByteArray()));

        contextByteStream.close();
        contextOutStream.close();

        // Reset the variables
        NUM_POSTINGS.set(0);
        ID_PREV.set(0);
        POSTING_BYTE_STREAM.reset();
      }

      int numPostings = NUM_POSTINGS.get();
      while (iter.hasNext()) {
        int gap = key.getRightElement() - ID_PREV.get();

        // write the doc id gap
        WritableUtils.writeVInt(POSTING_OUT_STREAM, gap);
        // write the posting list
        WritableUtils.writeVInt(POSTING_OUT_STREAM, iter.next().get());

        ID_PREV.set(key.getRightElement());
        numPostings++;
      }

      NUM_POSTINGS.set(numPostings);
      TERM.set(key.getLeftElement());
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{
        // Emit last posting
        POSTING_OUT_STREAM.flush();
        POSTING_BYTE_STREAM.flush();

        ByteArrayOutputStream contextByteStream = new ByteArrayOutputStream(4 + POSTING_BYTE_STREAM.size());
        DataOutputStream contextOutStream = new DataOutputStream(contextByteStream);
        WritableUtils.writeVInt(contextOutStream, NUM_POSTINGS.get());
        contextOutStream.write(POSTING_BYTE_STREAM.toByteArray());
        context.write(TERM, new BytesWritable(contextByteStream.toByteArray()));

        contextByteStream.close();
        contextOutStream.close();
        POSTING_BYTE_STREAM.close();
        POSTING_OUT_STREAM.close(); 
    }
  }

  private static final class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private BuildInvertedIndexCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;
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

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
