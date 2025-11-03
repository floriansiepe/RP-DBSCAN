package dm.kaist.algorithm;

import dm.kaist.dictionary.ApproximatedCell;
import dm.kaist.dictionary.Cell;
import dm.kaist.graph.Edge;
import dm.kaist.graph.LabeledCell;
import dm.kaist.io.ApproximatedPoint;
import dm.kaist.io.Point;
import org.apache.commons.lang3.ObjectUtils.Null;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import scala.Tuple2$;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public final class Conf implements Serializable {


    //input parameters (now instance fields)
    public String inputPath = null;
    public String metaOutputPath = null;
    public String pairOutputPath = null;
    public int numOfPartitions = 0;
    public float rho = 0;
    public int dim = 0;
    public int minPts = 0;
    public float epsilon = 0;
    public int metaBlockWindow = 1;
    public boolean boost = false;

    //parameters (now instance fields)
    public String delimeter = ",";
    public String metaFoler = "DICTIONARY";
    public String convertTableFolder = "CONVERTS";
    public String coreInfoFolder = "CORE_INFO";
    public String metaResult = "GLOBAL_CELL_GRAPH";
    public int limitDimForVirtualCombining = 6;
    public int numOflvhCellsInMetaPartition = 12000000;
    public int limitNumOflv1Cell = 1000000;
    public String metricsPath = "";

    // default constructor
    public Conf() {
    }

    public void setInputParameters(String[] args) {
        parseInputForm(args);

        if (inputPath == null || metaOutputPath == null || numOfPartitions <= 1 || rho == 0 || dim == 0 || minPts == 0 || epsilon == 0) {
            System.out.println("Your command must include the necessary parameters properly.");
            System.out.println("1. Necessary parameters");
            System.out.println(" -i : the hdfs path for input dataset.");
            System.out.println(" -o : the local path to write the meta result of clustering (e.g., # of (sub-)cells, # of points for each cluster).");
            System.out.println(" -np : the number of cores or partitions which you want to set.");
            System.out.println(" -rho : the approximation parameter.");
            System.out.println(" -dim : the number of dimensions.");
            System.out.println(" -minPts : the minimum number of neighbor points.");
            System.out.println(" -eps : the radius of a neighborhood.");
            System.out.println(" -M : Metrics output path (local file system).");
            System.out.println("2. Optional parameters");
            System.out.println(" -bs : the block size for virtually combining two-level cell dictionary (default : 1).");
            System.out.println(" -l : the hdfs path to write labeled points, <point id, cluster label> (default : no output).");
            System.exit(1);
        } else if (rho < 0.009999) {
            System.out.println("This version deos not support rho < 0.01!. We recommend you to set rho = 0.02 or 0.01 for accuracy.");
            System.exit(1);
        }
    }

    public void parseInputForm(String[] args) {
        if (args.length % 2 != 0)
            System.out.println("Argument parsing error.");

        String header = "";
        String value = "";
        for (int i = 0; i < args.length; i += 2) {
            header = args[i];
            value = args[i + 1];

            if (header.equals("-i"))
                inputPath = value;
            else if (header.equals("-o"))
                metaOutputPath = value;
            else if (header.equals("-l"))
                pairOutputPath = value;
            else if (header.equals("-np"))
                numOfPartitions = Integer.parseInt(value);
            else if (header.equals("-rho"))
                rho = Float.parseFloat(value);
            else if (header.equals("-dim"))
                dim = Integer.parseInt(value);
            else if (header.equals("-minPts"))
                minPts = Integer.parseInt(value);
            else if (header.equals("-eps"))
                epsilon = Float.parseFloat(value);
            else if (header.equals("-M"))
                metricsPath = value;
            else if (header.equals("-bs"))
                metaBlockWindow = Integer.parseInt(value);
                //We are now testing this code to boost our algorithm.
            else if (header.equals("-boost")) {
                if (value != null && value.equalsIgnoreCase("true"))
                    boost = true;
            } else {
                System.out.println("Argument parsing error!");
                System.exit(1);
            }
        }

    }

}
