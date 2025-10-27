package dm.kaist.io;

import dm.kaist.algorithm.Conf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

public class FileIO {
    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }

    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }

    public static void refreshFolder(Configuration conf, Conf config) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        //refresh folder and sub files
        Path metaPath = new Path(config.metaFoler);
        if (fs.exists(metaPath))
            fs.delete(metaPath, true);
        fs.mkdirs(metaPath);

        Path convertTablePath = new Path(config.convertTableFolder);
        if (fs.exists(convertTablePath))
            fs.delete(convertTablePath, true);
        fs.mkdirs(convertTablePath);

        Path coreIdsPath = new Path(config.coreInfoFolder);
        if (fs.exists(coreIdsPath))
            fs.delete(coreIdsPath, true);
        fs.mkdirs(coreIdsPath);

        Path metaResultPath = new Path(config.metaResult);
        if (fs.exists(metaResultPath))
            fs.delete(metaResultPath, true);
        fs.mkdirs(metaResultPath);

        if (config.pairOutputPath != null) {
            Path writeResultPath = new Path(config.pairOutputPath);
            if (fs.exists(writeResultPath))
                fs.delete(writeResultPath, true);
            fs.mkdirs(writeResultPath);
        }
    }

    public static List<String> broadCastData(JavaSparkContext sc, Configuration conf, String dirPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(dirPath));
        List<String> metaPaths = new ArrayList<String>();

        long size = 0;

        for (int i = 0; i < status.length; i++) {
            String path = status[i].getPath().toString();
            String fileName = status[i].getPath().getName();
            sc.addFile(path);
            metaPaths.add(fileName);

            size += status[i].getLen();
        }

        System.out.println("size : " + size);

        return metaPaths;
    }
}
