package metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import metrics.entity.ClusterParameters;
import metrics.entity.DatasetParameters;
import metrics.entity.Measurement;

import java.nio.file.Files;
import java.nio.file.Path;

public class MetricWriter {
    private final Path basePath;

    public MetricWriter(Path basePath) {
        this.basePath = basePath;
    }

    public <T extends ClusterParameters, U extends DatasetParameters> void writeMetrics(Measurement<T, U> measurement) {
        var objectMapper = new ObjectMapper();
        try {
            String jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(measurement);
            var subDir = measurement.datasetParameters().datasetName() + "/" + measurement.algo() + "/" + measurement.clusterParameters().eps() + "_" + measurement.clusterParameters().minPts();
            var fullPath = basePath.resolve(subDir);
            Files.createDirectories(fullPath);
            var filePath = fullPath.resolve("metrics.json");
            Files.writeString(
                    filePath,
                    jsonString
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
