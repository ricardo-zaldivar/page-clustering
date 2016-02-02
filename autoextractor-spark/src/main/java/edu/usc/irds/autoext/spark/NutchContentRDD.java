package edu.usc.irds.autoext.spark;

import edu.usc.irds.autoext.nutch.ContentIterator;
import edu.usc.irds.autoext.nutch.ContentPartition;
import edu.usc.irds.autoext.spark.utils.LangUtils;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
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

import java.util.List;
import java.util.function.Function;

/**
 * Creates Nutch Content RDD from a list of Sequence file paths
 * @author Thamme Gowda
 */
public class NutchContentRDD extends RDD<Content> {

    public static final Logger LOG = LoggerFactory.getLogger(NutchContentRDD.class);
    private static final ClassTag<Content> CONTENT_TAG = LangUtils.getClassTag(Content.class);

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
        super(context, new ArrayBuffer<>(), CONTENT_TAG);
        this.partitions = new ContentPartition[parts.size()];
        this.contentTypeFilter = contentTypeFilter;
        for (int i = 0; i < parts.size(); i++) {
            partitions[i] = new ContentPartition(i, parts.get(i));
        }

    }

    @Override
    public Iterator<Content> compute(Partition split, TaskContext context) {
        try {
            Path path = new Path(partitions[split.index()].getPath());
            return new ContentIterator(path, NutchConfiguration.create(), contentTypeFilter);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
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
