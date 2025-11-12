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
            // Skip directories, temporary and hidden files (e.g. _SUCCESS) and zero-length files
            if (status[i].isDirectory()) {
                continue;
            }
            String fileName = status[i].getPath().getName();
            if (fileName == null) continue;
            if (fileName.startsWith("_") || fileName.startsWith(".")) {
                // skip Hadoop marker files and hidden files
                System.out.println("Skipping broadcast of file: " + fileName);
                continue;
            }
            long len = status[i].getLen();
            if (len == 0) {
                System.out.println("Skipping zero-length file: " + fileName);
                continue;
            }

            // Verify file is gzip-compressed by checking magic bytes before broadcasting
            boolean looksLikeGzip = false;
            try (org.apache.hadoop.fs.FSDataInputStream in = fs.open(status[i].getPath())) {
                int b1 = in.read();
                int b2 = in.read();
                if (b1 == 0x1f && b2 == 0x8b) {
                    looksLikeGzip = true;
                }
            } catch (IOException e) {
                System.out.println("Failed to read header of file " + fileName + ": " + e.getMessage());
                continue;
            }

            if (!looksLikeGzip) {
                System.out.println("Skipping non-gzip file: " + fileName);
                continue;
            }

            String path = status[i].getPath().toString();
            sc.addFile(path);
            metaPaths.add(fileName);

            size += len;
        }

        System.out.println("size : " + size);

        return metaPaths;
    }
}
