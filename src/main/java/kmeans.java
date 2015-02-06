import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.ClusterClassifier;
import org.apache.mahout.clustering.iterator.ClusterIterator;
import org.apache.mahout.clustering.iterator.KMeansClusteringPolicy;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.Vector;

import com.google.common.collect.Lists;

public class kmeans {
  public static void main(String[] args) throws Exception {
    DistanceMeasure measure = new ManhattanDistanceMeasure();
    Path samples = new Path(args[0]);
    Path output = new Path(args[1]);
    Configuration conf = new Configuration();
    HadoopUtil.delete(conf, samples);
    HadoopUtil.delete(conf, output);
    RandomUtils.useTestSeed();
    double convergenceDelta = 0.001;
    int numClusters = 3;
    int maxIterations = 10;
    runSequentialKMeansClusterer(conf, samples, output, measure, numClusters, maxIterations, convergenceDelta);
  }
  
  
  private static void runSequentialKMeansClusterer(Configuration conf, Path samples, Path output,
    DistanceMeasure measure, int numClusters, int maxIterations, double convergenceDelta)
    throws IOException, InterruptedException, ClassNotFoundException {
    Path clustersIn = new Path(output, "random-seeds");
    RandomSeedGenerator.buildRandom(conf, samples, clustersIn, numClusters, measure);
    KMeansDriver.run(samples, clustersIn, output, convergenceDelta, maxIterations, true, 0.0, true);
  }

}