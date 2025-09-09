package dm.kaist.algorithm;

import dm.kaist.dictionary.ApproximatedCell;
import dm.kaist.graph.Edge;
import dm.kaist.io.ApproximatedPoint;
import dm.kaist.io.FileIO;
import dm.kaist.io.SerializableConfiguration;
import dm.kaist.partition.Partition;
import org.apache.commons.lang3.ObjectUtils.Null;
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
import java.util.List;

public class RP_DBSCAN implements Serializable {

    public JavaSparkContext sc = null;
    public SerializableConfiguration conf = null;
    public JavaPairRDD<Integer, ApproximatedCell> dataset = null;
    public List<String> metaPaths = null;
    public List<String> corePaths = null;
    public JavaPairRDD<Integer, Edge> edgeSet = null;

    //meta result of RP-DBSCAN
    public long numOfCells = 0;
    public long numOfSubCells = 0;
    public int numOfSubDictionaries = 0;
    public long numOfCorePoints = 0;
    public long numOfClusters = 0;
    public List<Tuple2<Integer, Long>> numOfPtsInCluster = null;

    public RP_DBSCAN(JavaSparkContext sc) {
        this.sc = sc;
        this.conf = new SerializableConfiguration();

        this.initialization(conf);
    }

    /**
     * Refresh folders and files for current execution.
     *
     * @param conf
     */
    public void initialization(SerializableConfiguration conf) {
        try {
            //Refresh Folder and Files
            FileIO.refreshFolder(conf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Phase I : pre-processing for RP-DBSCAN.
     * Phase I-1 (Pseudo Random Partitioning) and Phase I-2 (Cell_Dictionary_Building & Broadcasting)
     */
    public void phaseI() {
        /**
         * Phase I-1. Pseudo Random Partitioning
         */

        //Read input data set from HDFS
        JavaRDD<String> lines = sc.textFile(Conf.inputPath, Conf.numOfPartitions);
        JavaPairRDD<List<Integer>, ApproximatedCell> dataMap = null;

        //Data partitioning
        if (Conf.boost) {
            dataMap = lines.zipWithIndex()
                    .mapToPair(tuple -> new Methods.PointToCell(Conf.dim, Conf.epsilon, tuple._2).call(tuple._1))
                    .combineByKey(new Methods.CreateLocalApproximatedPoint(Conf.dim, Conf.epsilon, Conf.rho), new Methods.LocalApproximation(Conf.dim, Conf.epsilon, Conf.rho), new Methods.GlobalApproximation(Conf.dim))
                    .mapToPair(new Methods.PseudoRandomPartition2(Conf.metaBlockWindow)).persist(StorageLevel.MEMORY_AND_DISK_SER());
        } else
            dataMap = lines.zipWithIndex()
                    .mapToPair(tuple -> new Methods.PointToCell(Conf.dim, Conf.epsilon, tuple._2).call(tuple._1))
                    .groupByKey()
                    .mapToPair(new Methods.PseudoRandomPartition(Conf.dim, Conf.epsilon, Conf.rho, Conf.metaBlockWindow, Conf.pairOutputPath))
                    .persist(StorageLevel.MEMORY_AND_DISK_SER());
        numOfCells = dataMap.count();

        /**
         * Phase I-2. Cell_Dictionary_Building & Broadcasting
         */
        //Dictionary Defragmentation
        JavaPairRDD<List<Integer>, Long> ptsCountforEachMetaBlock = dataMap.mapToPair(new Methods.MetaBlockMergeWithApproximation()).reduceByKey(new Methods.AggregateCount());
        List<Tuple2<List<Integer>, Long>> numOfPtsInCell = ptsCountforEachMetaBlock.collect();
        //System.out.println("# of Blocks for virtually combining : " + numOfPtsInCell.size());

        HashMap<List<Integer>, List<Integer>> partitionIndex = new HashMap<List<Integer>, List<Integer>>();
        Tuple2<Long, List<Partition>> metaInfoForVirtualCombining = Methods.scalablePartition(numOfPtsInCell, Conf.dim, Conf.numOflvhCellsInMetaPartition / Conf.dim, partitionIndex);
        numOfSubCells = metaInfoForVirtualCombining._1;
        List<Partition> wholePartitions = metaInfoForVirtualCombining._2;
        numOfSubDictionaries = wholePartitions.size();

        //Build Two-Level Cell Dictionary composed of multiple sub-dictionaries
        JavaPairRDD<Integer, Iterable<ApproximatedCell>> evenlySplitPartitions = dataMap.flatMapToPair(new Methods.AssignApproximatedPointToPartition(partitionIndex)).groupByKey(wholePartitions.size());
        JavaPairRDD<Null, Null> metaDataSet = evenlySplitPartitions.mapToPair(new Methods.MetaGenerationWithApproximation(Conf.dim, Conf.epsilon, Conf.rho, Conf.minPts, conf, wholePartitions));
        metaDataSet.collect();

        //Re-partition the pseudo random partitions into Each Worker by a randomly assigned integer value for reducing the size of memory usage.
        dataset = dataMap.mapToPair(new Methods.Repartition(Conf.numOfPartitions)).repartition(Conf.numOfPartitions).persist(StorageLevel.MEMORY_AND_DISK_SER());

        //Broadcast two-level cell dictionary to every workers.
        try {
            metaPaths = FileIO.broadCastData(sc, conf, Conf.metaFoler);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Phase II : local clustering for RP-DBSCAN.
     * Phase II-1 (Core Marking) and Phase II-2 (Subgraph Building)
     */
    public void phaseII() {
        /**
         * Phase II-1: Core Marking
         */

        //Mark core cells and core points with the (eps,rho)-region query.
        JavaPairRDD<Long, ApproximatedCell> coreCells = dataset.mapPartitionsToPair(new Methods.FindCorePointsWithApproximation(Conf.dim, Conf.epsilon, Conf.minPts, conf, metaPaths)).persist(StorageLevel.MEMORY_AND_DISK_SER());

        //Count the number of core cells
        List<Tuple2<Integer, Long>> numOfCores = coreCells.mapToPair(new Methods.CountCorePts()).reduceByKey(new Methods.AggregateCount()).collect();
        numOfCorePoints = numOfCores.get(0)._2;

        //Broadcast core cell ids to every workers for updating the status of edges in cell subgraphs.
        try {
            corePaths = FileIO.broadCastData(sc, conf, Conf.coreInfoFolder);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        /**
         * Phase II-2: Subgraph Building
         */
        // Build cell subgraph
        edgeSet = coreCells.mapPartitionsToPair(new Methods.FindDirectDensityReachableEdgesWithApproximation(Conf.dim, Conf.epsilon, Conf.minPts, conf, metaPaths, corePaths, Conf.numOfPartitions)).repartition(Conf.numOfPartitions / 2);

    }

    /**
     * Phase III : post-processing for RP-DBSCAN
     * Phase III-1 (Progressive Graph Merging) and Phase III-2 (Point Labeling)
     */
    public void phaseIII() {
        /**
         * Phase III-1: Progressive Graph Merging
         */

        // Merge subgraphs into global cell graph through following parallel procedures: Single Merger, Edge Type Detection and Edge Reduction.
        int curPartitionSize = Conf.numOfPartitions;
        while (curPartitionSize != 1) {
            curPartitionSize = curPartitionSize / 2;
            edgeSet = edgeSet.mapPartitionsToPair(new Methods.BuildMST(conf, corePaths, curPartitionSize)).repartition(curPartitionSize);
        }

        List<Tuple2<Integer, Integer>> result = edgeSet.mapPartitionsToPair(new Methods.FinalPhase(conf, corePaths)).collect();

        // Count the number of Cluster in global cell graph.
        numOfClusters = result.get(0)._2;

        /**
         * Phase III-2: Point Labeling
         */
        //Assign border points into proper clusters (partially condition of Theorem 3.5).
        JavaPairRDD<Integer, ApproximatedPoint> borderPts = dataset.flatMapToPair(new Methods.EmitConnectedCoreCellsFromBorderCell(conf, Conf.numOfPartitions)).groupByKey().flatMapToPair(new Methods.AssignBorderPointToCluster(Conf.dim, Conf.epsilon, conf, Conf.pairOutputPath));

        //Assign core points into proper clusters (fully condition of Theorem 3.5.
        JavaPairRDD<Integer, ApproximatedPoint> corePts = dataset.mapPartitionsToPair(new Methods.AssignCorePointToCluster(conf, Conf.pairOutputPath));

        //Point labeling algorithm 1 : faster than algorithm 2, but not scalable.
        //If out-of-memory error is occurred during the labeling procedure, then use below algorithm 2 for labeling instead of this.
        //union the two results.
        JavaPairRDD<Integer, ApproximatedPoint> assignedResult = borderPts.union(corePts);

        //count the number of points in each cluster.
        numOfPtsInCluster = assignedResult.mapPartitionsToPair(new Methods.CountForEachCluster()).reduceByKey(new Methods.AggregateCount()).collect();
		
		
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
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(Conf.metaOutputPath))) {
            // Prepare headers and values
            String[] headers = {
                    "Input", "Output", "NumPartitions", "Rho", "Dim", "Epsilon", "MinPts", "MetaBlockWindow"
            };
            String[] values = {
                    Conf.inputPath,
                    Conf.metaOutputPath,
                    String.valueOf(Conf.numOfPartitions),
                    String.valueOf(Conf.rho),
                    String.valueOf(Conf.dim),
                    String.valueOf(Conf.epsilon),
                    String.valueOf(Conf.minPts),
                    String.valueOf(Conf.metaBlockWindow)
            };

            // Add optional PairOutputPath
            StringBuilder headerLine = new StringBuilder();
            StringBuilder valueLine = new StringBuilder();
            for (int i = 0; i < headers.length; i++) {
                headerLine.append(headers[i]).append(",");
                valueLine.append(values[i]).append(",");
            }
            if (Conf.pairOutputPath != null) {
                headerLine.append("PairOutputPath,");
                valueLine.append(Conf.pairOutputPath).append(",");
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
        }    }
}
