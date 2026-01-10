package com.example.domain;

/**
 * Type-safe identifier for a position.
 * Represents the unique combination of account and instrument.
 */
public record PositionId(
    String accountId,
    String instrumentId
) {

    public PositionId {
        if (accountId == null || accountId.isBlank()) {
            throw new IllegalArgumentException("Account ID cannot be null or blank");
        }
        if (instrumentId == null || instrumentId.isBlank()) {
            throw new IllegalArgumentException("Instrument ID cannot be null or blank");
        }
    }

    /**
     * @return Position identifier in the format "{accountId}-{instrumentId}"
     * Used for entity IDs and logging.
     */
    public String toEntityId(String batchId) {
        return batchId + "#" + accountId + "#" + instrumentId;
    }

    /**
     * Creates a PositionId from an entity ID string.
     * @param entityId the entity ID in format "{accountId}-{instrumentId}"
     * @return PositionId parsed from the string
     * @throws IllegalArgumentException if the format is invalid
     */
    public static PositionId fromEntityId(String entityId) {
        if (entityId == null || entityId.isBlank()) {
            throw new IllegalArgumentException("Entity ID cannot be null or blank");
        }

        var parts = entityId.split("#", 3);
        if (parts.length != 3 || parts[1].isBlank() || parts[2].isBlank()) {
            throw new IllegalArgumentException("Invalid entity ID format. Expected: accountId-instrumentId");
        }

        return new PositionId(parts[1], parts[2]);
    }
}