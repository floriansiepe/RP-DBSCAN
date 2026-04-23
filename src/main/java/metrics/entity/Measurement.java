package metrics.entity;

import java.util.Map;

public record Measurement<T extends ClusterParameters, U extends DatasetParameters>(
        String algo,
        long algoTimeMs,
        T clusterParameters,
        U datasetParameters,
        Map<String, Object> additionalMetrics) {
}

