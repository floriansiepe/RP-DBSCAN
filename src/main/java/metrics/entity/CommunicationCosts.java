package metrics.entity;

public record CommunicationCosts(
        long partitioningPointsExchanged,   // Number of approximated points shuffled in Phase I
        long mergingEdgesExchanged          // Number of edges shuffled in Phase III
) {}


