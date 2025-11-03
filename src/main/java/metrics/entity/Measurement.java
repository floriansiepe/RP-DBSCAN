package metrics.entity;

public record Measurement<T extends ClusterParameters, U extends DatasetParameters>(
        String algo,
        long algoTimeMs,
        T clusterParameters,
        U datasetParameters) {
}

