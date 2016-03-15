package edu.usc.irds.autoext.spark;

import edu.usc.irds.autoext.nutch.ContentIterator;
import edu.usc.irds.autoext.nutch.ContentPartition;
import edu.usc.irds.autoext.spark.utils.LangUtils;
import edu.usc.irds.lang.Function;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag;

import java.util.Collections;
import java.util.List;

/**
 * Creates Nutch Content RDD from a list of Sequence file paths
 * @author Thamme Gowda
 */
public class NutchContentRDD extends RDD<Content> {

    public static final Logger LOG = LoggerFactory.getLogger(NutchContentRDD.class);
    private static final ClassTag<Content> CONTENT_TAG = LangUtils.getClassTag(Content.class);

    /**
     * In defensive mode, this RDD ignores errors and tries
     * to do the best by ignoring errors
     */
    private boolean defensive = true;
    private final Function<String, Boolean> contentTypeFilter;
    private final ContentPartition[] partitions;

    /**
     * Creates Nutch Content RDD
     * @param context the spark Context
     * @param parts list of paths to nutch segment content data
     */
    public NutchContentRDD(SparkContext context,
                           List<String> parts,
                           Function<String, Boolean> contentTypeFilter) {
        super(context, new ArrayBuffer<Dependency<?>>(), CONTENT_TAG);
        this.partitions = new ContentPartition[parts.size()];
        this.contentTypeFilter = contentTypeFilter;
        for (int i = 0; i < parts.size(); i++) {
            partitions[i] = new ContentPartition(i, parts.get(i));
        }
    }

    @Override
    public Iterator<Content> compute(Partition split, TaskContext context) {
        Path path = new Path(partitions[split.index()].getPath());
        try {
            LOG.info("Reading {}", path);
            return new ContentIterator(path, NutchConfiguration.create(), contentTypeFilter);
        } catch (Exception e) {
            if (defensive) {
                LOG.error("Skipped: {} due to {}", path, e.getMessage());
                //Stick an empty buffer
                return new ArrayBuffer<Content>().iterator();
            } else { // break
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Partition[] getPartitions() {
        return this.partitions;
    }

    public JavaRDD<Content> toJavaRDD(){
        return new JavaRDD<>(this, CONTENT_TAG);
    }
}
