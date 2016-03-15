package edu.usc.irds.autoext.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.Method;


public class RecCounterMR extends RecCounter {

    public static class CounterMapper extends
            Mapper<Writable, Writable, Text, LongWritable> {

        public static final LongWritable ONE = new LongWritable(1);

        private Text filename;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit split = context.getInputSplit();
            Class<? extends InputSplit> splitClass = split.getClass();

            FileSplit fileSplit = null;
            if (splitClass.equals(FileSplit.class)) {
                fileSplit = (FileSplit) split;
            } else if (splitClass.getName().equals(
                    "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
                // begin reflection hackery...

                try {
                    Method getInputSplitMethod = splitClass
                            .getDeclaredMethod("getInputSplit");
                    getInputSplitMethod.setAccessible(true);
                    fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
                } catch (Exception e) {
                    // wrap and re-throw error
                    throw new IOException(e);
                }
                // end reflection hackery
            }
            filename = new Text(fileSplit.getPath().toString());
        }

        public void map(Writable key, Writable value, Context context)
                throws IOException, InterruptedException {
            context.write(filename, ONE);
        }
    }

    public static class CounterReducer extends
            Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context )
                throws IOException, InterruptedException {
            long count = 0;
            for(LongWritable value: values) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    private int run() throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(RecCounterMR.class);
        job.setJobName("Record Counter");
        job.setJarByClass(RecCounterMR.class);

        for (String path : paths) {
            MultipleInputs.addInputPath(job,new Path(path),
                    SequenceFileInputFormat.class, CounterMapper.class);
        }
        Path resultPath = new Path(outPath == null ? "reccount-out" : outPath);
        FileOutputFormat.setOutputPath(job, resultPath);
        job.setReducerClass(CounterReducer.class);

        //job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        RecCounterMR instance = new RecCounterMR();
        instance.parseArgs(args);
        instance.run();
    }
}
