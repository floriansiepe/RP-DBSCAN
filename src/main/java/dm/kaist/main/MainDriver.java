package dm.kaist.main;

import dm.kaist.algorithm.Conf;
import dm.kaist.algorithm.RP_DBSCAN;
import dm.kaist.dictionary.ApproximatedCell;
import dm.kaist.dictionary.Cell;
import dm.kaist.graph.Edge;
import dm.kaist.graph.LabeledCell;
import dm.kaist.io.ApproximatedPoint;
import dm.kaist.io.Point;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple2$;

import java.util.ArrayList;
import java.util.HashMap;

public class MainDriver {

    public static SparkConf setSparkConfiguration(String numOfInstance, String numOfCore, String exeMemory, String driverMemory, String overHeap) {
        SparkConf sparkConf = new SparkConf().setAppName("RP_DBSCAN");
        sparkConf.set("spark.executor.instances", numOfInstance);
        sparkConf.set("spark.executor.cores", numOfCore);
        sparkConf.set("spark.executor.memory", exeMemory);
        sparkConf.set("spark.driver.memory", driverMemory);
        sparkConf.set("spark.driver.maxResultSize", "8g");
        sparkConf.set("spark.yarn.submit.file.replication", "2");
        sparkConf.set("spark.yarn.driver.memoryOverhead", overHeap);
        sparkConf.set("spark.yarn.executor.memoryOverhead", overHeap);
        sparkConf.set("spark.shuffle.service.enabled", "true");
        sparkConf.set("spark.shuffle.memoryFraction", "0.5");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryoserializer.buffer.mb", "256");
        sparkConf.set("spark.memory.fraction", "0.7");
        sparkConf.registerKryoClasses(new Class<?>[]{ArrayList.class, Edge.class, Point.class, ObjectUtils.Null.class, Cell.class, ApproximatedCell.class, ApproximatedPoint.class, LabeledCell.class, HashMap.class});
        return sparkConf;
    }

    public static SparkConf setSparkConfiguration() {
        SparkConf sparkConf = new SparkConf().setAppName("RP_DBSCAN");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryoserializer.buffer.mb", "256");
        sparkConf.registerKryoClasses(new Class<?>[]{ArrayList.class, Edge.class, Point.class, ObjectUtils.Null.class, Cell.class, ApproximatedCell.class, ApproximatedPoint.class, LabeledCell.class, HashMap.class, Tuple2.class, Tuple2$.class});
        return sparkConf;
    }
    /**
     * @author Hwanjun Song(KAIST), Jae-Gil Lee(KAIST)
     * Created on 18/03/02
     * To find clusters using RP-DBSCAN
     **/
    public static void main(String[] args) {
        System.out.println("ARGS");
        for (String arg : args)
            System.out.println(arg);
        //Parameter Load
        var conf = new Conf();
        conf.setInputParameters(args);

        //Spark Configuration
        var sparkConf = setSparkConfiguration();

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        long start, end;
        start = System.currentTimeMillis();

        RP_DBSCAN rp_dbscan = new RP_DBSCAN(sc, conf);

        //PHASE I: Data Partitioning
        rp_dbscan.phaseI();

        //PHASE II: Cell Graph Construction
        rp_dbscan.phaseII();

        //PHASE III: Cell Graph Merging
        rp_dbscan.phaseIII();

        end = System.currentTimeMillis();

        //Write meta results
        rp_dbscan.writeMetaResult((end - start));

        sc.close();
    }


}
