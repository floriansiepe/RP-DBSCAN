package dm.kaist.main;

import dm.kaist.algorithm.Conf;
import dm.kaist.algorithm.RP_DBSCAN;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class MainDriver {
    /**
     * @author Hwanjun Song(KAIST), Jae-Gil Lee(KAIST)
     * Created on 18/03/02
     * To find clusters using RP-DBSCAN
     **/
    public static void main(String[] args) {
        //Parameter Load
        Conf.setInputParameters(args);

        //You should change spark configurations to achieve the best performance with considering your system environment.
        SparkConf sparkConf = Conf.setSparkConfiguration("5", "4", "20g", "10g", "2048");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        long start, end;
        start = System.currentTimeMillis();

        RP_DBSCAN rp_dbscan = new RP_DBSCAN(sc);

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
