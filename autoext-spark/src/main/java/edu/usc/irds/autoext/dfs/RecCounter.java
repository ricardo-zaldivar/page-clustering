package edu.usc.irds.autoext.dfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;

/**
 * An utility to count number of records in a sequence file
 */
public class RecCounter {

    @Option(name = "-list", forbids = {"-in"},
            usage = "List of Sequence Files")
    private String pathListFile = null;

    @Option(name = "-out", usage = "Output file path.")
    private String outPath = null;

    @Option(name = "-in", forbids = {"-list"})
    private String inPath = null;

    private Configuration conf;

    /**
     * Counts records in a sequence file
     * @param path path to sequence file
     * @return number of records
     * @throws IOException when an error occurs
     */

    private long countRecords(Path path) throws IOException {
        try(SequenceFile.Reader reader =
                    new SequenceFile.Reader(conf, SequenceFile.Reader.file(path))) {
            Writable keyInstance = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            long count = 0;
            while (reader.next(keyInstance)) {
                count++;
            }
            return count;
        }
    }

    private void count() throws IOException {

        this.conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        List<String> paths;
        if (pathListFile != null) {
            try(FSDataInputStream stream = fs.open(new Path(pathListFile))){
                paths = IOUtils.readLines(stream);
            }
        } else if (inPath != null) {
            paths = new ArrayList<>();
            paths.add(inPath);
        } else {
            throw new RuntimeException("this shouldn't be happening!");
        }
        try (PrintWriter out = outPath == null ?
                new PrintWriter(System.out) :
                new PrintWriter(fs.create(new Path(outPath), true))){
            for (String path : paths) {
                try {
                    long count = countRecords(new Path(path));
                    out.println(path + "\t" + count);
                } catch (Throwable t){
                    out.println(path + "\t" + "ERROR : " + t.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        RecCounter counter = new RecCounter();
        CmdLineParser parser = new CmdLineParser(counter);
        try {
            parser.parseArgument(args);
            if (counter.inPath == null && counter.pathListFile == null){
                throw new CmdLineException("Either -list or -in is required");
            }
        } catch (CmdLineException e) {
            parser.printUsage(System.out);
            System.exit(-1);
        }
        counter.count();
    }

}
