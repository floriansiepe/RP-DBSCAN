package dm.kaist.algorithm;

import dm.kaist.dictionary.ApproximatedCell;
import dm.kaist.dictionary.Dictionary;
import dm.kaist.graph.Cluster;
import dm.kaist.graph.Edge;
import dm.kaist.io.ApproximatedPoint;
import dm.kaist.io.FileIO;
import dm.kaist.io.SerializableConfiguration;
import dm.kaist.partition.Partition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class RP_DBSCAN implements Serializable {

    public Conf config;
    public JavaSparkContext sc = null;
    public SerializableConfiguration conf = null;
    public JavaPairRDD<Integer, ApproximatedCell> dataset = null;
    public List<Dictionary> metaPaths = null;
    public HashSet<Long> corePaths = null;
    public JavaPairRDD<Integer, Edge> edgeSet = null;

    //meta result of RP-DBSCAN
    public long numOfCells = 0;
    public long numOfSubCells = 0;
    public int numOfSubDictionaries = 0;
    public long numOfCorePoints = 0;
    public long numOfClusters = 0;
    public List<Tuple2<Integer, Long>> numOfPtsInCluster = null;

    public RP_DBSCAN(JavaSparkContext sc, Conf config) {
        this.sc = sc;
        this.conf = new SerializableConfiguration();
        this.config = config;

        this.initialization(conf, config);
    }

    /**
     * Refresh folders and files for current execution.
     *
     * @param conf
     * @param config
     */
    public void initialization(SerializableConfiguration conf, Conf config) {
        try {
            FileIO.refreshFolder(conf, config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Phase I : pre-processing for RP-DBSCAN.
     * Phase I-1 (Pseudo Random Partitioning) and Phase I-2 (Cell_Dictionary_Building & Broadcasting)
     */
    public void phaseI() {
        // Create local final copies of instance fields used inside lambdas to avoid capturing 'this'
        final Conf cfg = this.config;
        final SerializableConfiguration sconf = this.conf;

        /**
         * Phase I-1. Pseudo Random Partitioning
         */

        //Read input data set from HDFS
        JavaRDD<String> lines = sc.textFile(cfg.inputPath, cfg.numOfPartitions);
        JavaPairRDD<List<Integer>, ApproximatedCell> dataMap = null;

        //Data partitioning
        if (cfg.boost) {
            dataMap = lines.zipWithIndex()
                    .mapToPair(tuple -> new Methods.PointToCell(cfg.dim, cfg.epsilon, tuple._2, cfg.delimeter).call(tuple._1))
                    .combineByKey(k -> new Methods.CreateLocalApproximatedPoint(cfg.dim, cfg.epsilon, cfg.rho).call(k), (v, p) -> new Methods.LocalApproximation(cfg.dim, cfg.epsilon, cfg.rho).call(v, p), (l, r) -> new Methods.GlobalApproximation(cfg.dim, cfg.limitNumOflv1Cell).call(l, r))
                    .mapToPair(new Methods.PseudoRandomPartition2(cfg.metaBlockWindow, cfg.limitDimForVirtualCombining));
        } else
            dataMap = lines.zipWithIndex()
                    .mapToPair(tuple -> new Methods.PointToCell(cfg.dim, cfg.epsilon, tuple._2, cfg.delimeter).call(tuple._1))
                    .groupByKey()
                    .mapToPair(tuple -> new Methods.PseudoRandomPartition(cfg.dim, cfg.epsilon, cfg.rho, cfg.metaBlockWindow, cfg.pairOutputPath, cfg.limitDimForVirtualCombining).call(tuple));
        numOfCells = dataMap.count();
        System.out.println("# of Cells : " + numOfCells);
        System.out.println(dataMap.collect());

        /**
         * Phase I-2. Cell_Dictionary_Building & Broadcasting
         */
        //Dictionary Defragmentation
        JavaPairRDD<List<Integer>, Long> ptsCountforEachMetaBlock = dataMap.mapToPair(k -> new Methods.MetaBlockMergeWithApproximation(cfg.dim).call(k)).reduceByKey((x, y) -> new Methods.AggregateCount().call(x, y));
        List<Tuple2<List<Integer>, Long>> numOfPtsInCell = ptsCountforEachMetaBlock.collect();
        System.out.println("# of Blocks for virtually combining : " + numOfPtsInCell.size());
        for (Tuple2<List<Integer>, Long> entry : numOfPtsInCell) {
            if (entry._1.size() != cfg.dim) {
                throw new RuntimeException("Wrong number of blocks for virtually combining");
            }
        }


        HashMap<List<Integer>, List<Integer>> partitionIndex = new HashMap<List<Integer>, List<Integer>>();
        Tuple2<Long, List<Partition>> metaInfoForVirtualCombining = Methods.scalablePartition(numOfPtsInCell, cfg.dim, cfg.numOflvhCellsInMetaPartition / cfg.dim, partitionIndex, cfg.limitDimForVirtualCombining);
        numOfSubCells = metaInfoForVirtualCombining._1;
        List<Partition> wholePartitions = metaInfoForVirtualCombining._2;
        numOfSubDictionaries = wholePartitions.size();

        //Build Two-Level Cell Dictionary composed of multiple sub-dictionaries
        JavaPairRDD<Integer, Iterable<ApproximatedCell>> evenlySplitPartitions = dataMap.flatMapToPair(new Methods.AssignApproximatedPointToPartition(partitionIndex)).groupByKey(wholePartitions.size());
        JavaRDD<Dictionary> metaDataSet = evenlySplitPartitions.map(new Methods.MetaGenerationWithApproximation(cfg.dim, cfg.epsilon, cfg.rho, cfg.minPts, sconf, wholePartitions, cfg.metaFoler));
        this.metaPaths = metaDataSet.collect();

        //Re-partition the pseudo random partitions into Each Worker by a randomly assigned integer value for reducing the size of memory usage.
        dataset = dataMap.mapToPair(new Methods.Repartition(cfg.numOfPartitions)).repartition(cfg.numOfPartitions).persist(StorageLevel.MEMORY_AND_DISK_SER());
    }

    /**
     * Phase II : local clustering for RP-DBSCAN.
     * Phase II-1 (Core Marking) and Phase II-2 (Subgraph Building)
     */
    public void phaseII() {
        // Create local final copies of instance fields used inside lambdas to avoid capturing 'this'
        final Conf cfg = this.config;
        final SerializableConfiguration sconf = this.conf;
        final List<Dictionary> metaPathsLocal = this.metaPaths;
        /**
         * Phase II-1: Core Marking
         */

        //Mark core cells and core points with the (eps,rho)-region query.
        JavaPairRDD<Long, ApproximatedCell> coreCells = dataset.mapPartitionsToPair(new Methods.FindCorePointsWithApproximation(cfg.dim, cfg.epsilon, cfg.minPts, sconf, metaPathsLocal, cfg.coreInfoFolder)).persist(StorageLevel.MEMORY_AND_DISK_SER());

        corePaths = new HashSet<>(coreCells.map(x -> x._1).collect());
        //Count the number of core cells
        List<Tuple2<Integer, Long>> numOfCores = coreCells.mapToPair(new Methods.CountCorePts()).reduceByKey(new Methods.AggregateCount()).collect();
        numOfCorePoints = numOfCores.get(0)._2;


        /**
         * Phase II-2: Subgraph Building
         */
        // Build cell subgraph
        edgeSet = coreCells.mapPartitionsToPair(new Methods.FindDirectDensityReachableEdgesWithApproximation(cfg.dim, cfg.epsilon, cfg.minPts, sconf, metaPathsLocal, corePaths, cfg.numOfPartitions)).repartition(cfg.numOfPartitions / 2);

    }

    /**
     * Phase III : post-processing for RP-DBSCAN
     * Phase III-1 (Progressive Graph Merging) and Phase III-2 (Point Labeling)
     */
    public void phaseIII() {
        // Create local final copies of instance fields used inside lambdas to avoid capturing 'this'
        final Conf cfg = this.config;
        final SerializableConfiguration sconf = this.conf;
        final HashSet corePathsLocal = this.corePaths;

        /**
         * Phase III-1: Progressive Graph Merging
         */

        // Merge subgraphs into global cell graph through following parallel procedures: Single Merger, Edge Type Detection and Edge Reduction.
        int curPartitionSize = cfg.numOfPartitions;
        while (curPartitionSize != 1) {
            curPartitionSize = curPartitionSize / 2;
            edgeSet = edgeSet.mapPartitionsToPair(new Methods.BuildMST(sconf, corePathsLocal, curPartitionSize)).repartition(curPartitionSize);
        }

        List<Tuple2<Integer, List<Cluster>>> result = edgeSet.mapPartitionsToPair(new Methods.FinalPhase(sconf, corePathsLocal, cfg.metaResult)).collect();

        // Count the number of Cluster in global cell graph.
        var clusters = result.get(0)._2;
        numOfClusters = clusters.size();

        /**
         * Phase III-2: Point Labeling
         */
        //Assign border points into proper clusters (partially condition of Theorem 3.5).
        JavaPairRDD<Integer, ApproximatedPoint> borderPts = dataset.flatMapToPair(new Methods.EmitConnectedCoreCellsFromBorderCell(sconf, cfg.numOfPartitions, cfg.metaResult, clusters)).groupByKey().flatMapToPair(new Methods.AssignBorderPointToCluster(cfg.dim, cfg.epsilon, sconf, cfg.pairOutputPath, cfg.delimeter));

        //Assign core points into proper clusters (fully condition of Theorem 3.5.
        JavaPairRDD<Integer, ApproximatedPoint> corePts = dataset.mapPartitionsToPair(new Methods.AssignCorePointToCluster(sconf, cfg.pairOutputPath, cfg.metaResult, cfg.delimeter, clusters));

        //Point labeling algorithm 1 : faster than algorithm 2, but not scalable.
        //If out-of-memory error is occurred during the labeling procedure, then use below algorithm 2 for labeling instead of this.
        //union the two results.
        JavaPairRDD<Integer, ApproximatedPoint> assignedResult = borderPts.union(corePts);

        //count the number of points in each cluster.
        numOfPtsInCluster = assignedResult.mapPartitionsToPair(new Methods.CountForEachCluster()).reduceByKey(new Methods.AggregateCount()).collect();

        if (cfg.pairOutputPath != null) {
            JavaPairRDD<Long, Integer> clusterLabels = assignedResult.flatMapToPair(new Methods.FlatMapPointIdToClusterId());
            clusterLabels.saveAsTextFile(cfg.pairOutputPath);
        }


		/*
		// Point labeling algorithm 2 : scalable, but slower than algorithm 1.
		List<Tuple2<Integer, Long>> borderPtsList =  borderPts.mapPartitionsToPair(new Methods.CountForEachCluster()).reduceByKey(new Methods.AggregateCount()).collect();
		List<Tuple2<Integer, Long>> corePtsList =  corePts.mapPartitionsToPair(new Methods.CountForEachCluster()).reduceByKey(new Methods.AggregateCount()).collect();

		HashMap<Integer, Long> numOfPtsInCluster = new HashMap<Integer, Long>();
		for(Tuple2<Integer, Long> core : corePtsList)
			numOfPtsInCluster.put(core._1, core._2);
		for(Tuple2<Integer, Long> border : borderPtsList)
			numOfPtsInCluster.put( border._1 , numOfPtsInCluster.get(border._1)+border._2);

		for(Entry<Integer, Long> entry : numOfPtsInCluster.entrySet())
			System.out.println("CLUSTER ["+(entry.getKey()+1)+"] : "+ entry.getValue());
		*/


    }

    /**
     * Write Meta Result
     */
    public void writeMetaResult(long totalElapsedTime) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(config.metaOutputPath))) {
            // Prepare headers and values
            String[] headers = {
                    "Input", "Output", "NumPartitions", "Rho", "Dim", "Epsilon", "MinPts", "MetaBlockWindow"
            };
            String[] values = {
                    config.inputPath,
                    config.metaOutputPath,
                    String.valueOf(config.numOfPartitions),
                    String.valueOf(config.rho),
                    String.valueOf(config.dim),
                    String.valueOf(config.epsilon),
                    String.valueOf(config.minPts),
                    String.valueOf(config.metaBlockWindow)
            };

            // Add optional PairOutputPath
            StringBuilder headerLine = new StringBuilder();
            StringBuilder valueLine = new StringBuilder();
            for (int i = 0; i < headers.length; i++) {
                headerLine.append(headers[i]).append(",");
                valueLine.append(values[i]).append(",");
            }
            if (config.pairOutputPath != null) {
                headerLine.append("PairOutputPath,");
                valueLine.append(config.pairOutputPath).append(",");
            }

            // Add meta results
            headerLine.append("NumCells,NumSubCells,NumSubDictionaries,NumCorePoints,NumClusters,TotalElapsedTime(s),");
            valueLine.append(numOfCells).append(",")
                    .append(numOfSubCells).append(",")
                    .append(numOfSubDictionaries).append(",")
                    .append(numOfCorePoints).append(",")
                    .append(numOfClusters).append(",")
                    .append(totalElapsedTime / 1000.0).append(",");

            // Add cluster info
            for (int i = 0; i < numOfPtsInCluster.size(); i++) {
                headerLine.append("Cluster").append(i + 1).append(",");
                valueLine.append(numOfPtsInCluster.get(i)._2).append(",");
            }

            // Remove trailing commas
            if (headerLine.length() > 0) headerLine.setLength(headerLine.length() - 1);
            if (valueLine.length() > 0) valueLine.setLength(valueLine.length() - 1);

            // Write to file
            bw.write(headerLine.toString() + "\n");
            bw.write(valueLine.toString() + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
