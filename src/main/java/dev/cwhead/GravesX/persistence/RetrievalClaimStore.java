package dev.cwhead.GravesX.persistence;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Database-authoritative ownership for grave retrieval operations.
 *
 * <p>The store coordinates SQL state only. It intentionally does not claim to
 * make Minecraft player data, world saves, and SQL one atomic transaction.</p>
 */
public final class RetrievalClaimStore {
    public enum State { AVAILABLE, ACTIVE, QUARANTINED, DELETED }

    public enum Operation {
        INVENTORY_OPEN, AUTO_LOOT, BREAK_DROP, TIMEOUT_DROP, DELETE, ADDON_MUTATION
    }

    private static final Set<String> AVAILABLE_COLUMNS = Set.of(
            "inventory", "experience", "protection", "provider_id");

    public enum Denial { NOT_FOUND, ACTIVE, QUARANTINED, DATABASE_ERROR }

    public sealed interface AcquireResult permits Acquired, Denied {}

    public record Claim(
            UUID graveUuid,
            UUID token,
            UUID claimantUuid,
            String serverId,
            Operation operation) {}

    public record Payload(String inventory, String equipment, int experience) {}

    public record Acquired(Claim claim, Payload payload) implements AcquireResult {}

    public record Denied(
            Denial reason,
            String ownerServer,
            UUID ownerPlayer) implements AcquireResult {}

    @FunctionalInterface
    public interface AvailableMutation {
        boolean apply(Connection connection) throws SQLException;
    }

    private final DataSource dataSource;
    private final String graveTable;
    private final String claimTable;
    private final Clock clock;
    private final Runnable beforeClaimInsert;

    public RetrievalClaimStore(DataSource dataSource, String tablePrefix) {
        this(dataSource, tablePrefix, Clock.systemUTC(), () -> { });
    }

    RetrievalClaimStore(DataSource dataSource, String tablePrefix, Clock clock) {
        this(dataSource, tablePrefix, clock, () -> { });
    }

    RetrievalClaimStore(DataSource dataSource, String tablePrefix, Clock clock,
                        Runnable beforeClaimInsert) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        if (tablePrefix == null || !tablePrefix.matches("[A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Unsafe database table prefix");
        }
        this.graveTable = tablePrefix + "grave";
        this.claimTable = tablePrefix + "grave_claim";
        this.clock = Objects.requireNonNull(clock, "clock");
        this.beforeClaimInsert = Objects.requireNonNull(beforeClaimInsert, "beforeClaimInsert");
    }

    public void createTable() throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS " + claimTable + " ("
                + "grave_uuid VARCHAR(36) NOT NULL PRIMARY KEY, "
                + "state VARCHAR(16) NOT NULL, "
                + "claim_token VARCHAR(36), "
                + "claimant_uuid VARCHAR(36), "
                + "server_id VARCHAR(255), "
                + "operation VARCHAR(32), "
                + "claimed_at BIGINT, "
                + "updated_at BIGINT NOT NULL, "
                + "reason VARCHAR(255))";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    public int backfillAvailableClaims() throws SQLException {
        java.util.List<UUID> missing = new java.util.ArrayList<>();
        String select = "SELECT g.uuid FROM " + graveTable + " g "
                + "WHERE NOT EXISTS (SELECT 1 FROM " + claimTable + " c WHERE c.grave_uuid = g.uuid)";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(select);
             ResultSet rows = statement.executeQuery()) {
            while (rows.next()) missing.add(UUID.fromString(rows.getString(1)));
        }

        int ensured = 0;
        long now = clock.millis();
        for (UUID graveUuid : missing) {
            if (ensureClaimRow(graveUuid, now)) ensured++;
        }
        return ensured;
    }

    public boolean createAvailable(UUID graveUuid, AvailableMutation insertPayload) {
        Objects.requireNonNull(graveUuid, "graveUuid");
        Objects.requireNonNull(insertPayload, "insertPayload");
        long now = clock.millis();

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO " + claimTable
                                + " (grave_uuid, state, updated_at) VALUES (?, 'AVAILABLE', ?)")) {
                    statement.setString(1, graveUuid.toString());
                    statement.setLong(2, now);
                    if (statement.executeUpdate() != 1) {
                        connection.rollback();
                        return false;
                    }
                }
                if (!insertPayload.apply(connection) || !graveExists(connection, graveUuid)) {
                    connection.rollback();
                    return false;
                }
                connection.commit();
                return true;
            } catch (RuntimeException failure) {
                rollbackQuietly(connection);
                return false;
            } catch (Error failure) {
                rollbackQuietly(connection);
                throw failure;
            } catch (SQLException failure) {
                rollbackQuietly(connection);
                return false;
            } finally {
                restoreAutoCommitQuietly(connection);
            }
        } catch (SQLException failure) {
            return false;
        }
    }

    public AcquireResult acquire(
            UUID graveUuid,
            UUID claimantUuid,
            String serverId,
            Operation operation) {
        Objects.requireNonNull(graveUuid, "graveUuid");
        Objects.requireNonNull(serverId, "serverId");
        Objects.requireNonNull(operation, "operation");

        Claim claim = new Claim(graveUuid, UUID.randomUUID(), claimantUuid, serverId, operation);
        long now = clock.millis();

        beforeClaimInsert.run();
        if (!ensureClaimRow(graveUuid, now)) {
            return new Denied(Denial.DATABASE_ERROR, null, null);
        }

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (activateAvailable(connection, claim, now) != 1) {
                    Denied denial = readDenial(connection, graveUuid);
                    connection.rollback();
                    return denial != null
                            ? denial
                            : new Denied(Denial.DATABASE_ERROR, null, null);
                }

                Payload payload = readPayload(connection, graveUuid);
                if (payload == null) {
                    if (markDeleted(connection, claim, now) != 1) {
                        connection.rollback();
                        return new Denied(Denial.DATABASE_ERROR, null, null);
                    }
                    connection.commit();
                    return new Denied(Denial.NOT_FOUND, null, null);
                }

                connection.commit();
                return new Acquired(claim, payload);
            } catch (SQLException failure) {
                rollbackQuietly(connection);
                return new Denied(Denial.DATABASE_ERROR, null, null);
            } finally {
                restoreAutoCommitQuietly(connection);
            }
        } catch (SQLException failure) {
            return new Denied(Denial.DATABASE_ERROR, null, null);
        }
    }

    public boolean updateAvailableInventory(UUID graveUuid, String serializedInventory, int experience) {
        Objects.requireNonNull(graveUuid, "graveUuid");
        Objects.requireNonNull(serializedInventory, "serializedInventory");
        long now = clock.millis();
        if (!ensureClaimRow(graveUuid, now)) return false;

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (lockAvailable(connection, graveUuid, now) != 1) {
                    connection.rollback();
                    return false;
                }
                int changed;
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE " + graveTable + " SET inventory = ?, experience = ? WHERE uuid = ?")) {
                    statement.setString(1, serializedInventory);
                    statement.setInt(2, experience);
                    statement.setString(3, graveUuid.toString());
                    changed = statement.executeUpdate();
                }
                if (changed != 1) {
                    markAvailableDeleted(connection, graveUuid, now);
                    connection.commit();
                    return false;
                }
                connection.commit();
                return true;
            } catch (RuntimeException failure) {
                rollbackQuietly(connection);
                return false;
            } catch (Error failure) {
                rollbackQuietly(connection);
                throw failure;
            } catch (SQLException failure) {
                rollbackQuietly(connection);
                return false;
            } finally {
                restoreAutoCommitQuietly(connection);
            }
        } catch (SQLException failure) {
            return false;
        }
    }

    public boolean mutateAvailable(UUID graveUuid, AvailableMutation mutation) {
        Objects.requireNonNull(graveUuid, "graveUuid");
        Objects.requireNonNull(mutation, "mutation");
        long now = clock.millis();
        if (!ensureClaimRow(graveUuid, now)) return false;

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (lockAvailable(connection, graveUuid, now) != 1) {
                    connection.rollback();
                    return false;
                }
                if (!mutation.apply(connection) || !graveExists(connection, graveUuid)) {
                    connection.rollback();
                    return false;
                }
                connection.commit();
                return true;
            } catch (RuntimeException failure) {
                rollbackQuietly(connection);
                return false;
            } catch (Error failure) {
                rollbackQuietly(connection);
                throw failure;
            } catch (SQLException failure) {
                rollbackQuietly(connection);
                return false;
            } finally {
                restoreAutoCommitQuietly(connection);
            }
        } catch (SQLException failure) {
            return false;
        }
    }

    public boolean updateAvailableColumn(UUID graveUuid, String column, Object value) {
        Objects.requireNonNull(graveUuid, "graveUuid");
        if (!AVAILABLE_COLUMNS.contains(column)) {
            throw new IllegalArgumentException("Unsupported grave column: " + column);
        }
        long now = clock.millis();
        if (!ensureClaimRow(graveUuid, now)) return false;

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (lockAvailable(connection, graveUuid, now) != 1) {
                    connection.rollback();
                    return false;
                }
                int changed;
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE " + graveTable + " SET " + column + " = ? WHERE uuid = ?")) {
                    statement.setObject(1, value);
                    statement.setString(2, graveUuid.toString());
                    changed = statement.executeUpdate();
                }
                if (changed != 1) {
                    markAvailableDeleted(connection, graveUuid, now);
                    connection.commit();
                    return false;
                }
                connection.commit();
                return true;
            } catch (RuntimeException failure) {
                rollbackQuietly(connection);
                return false;
            } catch (Error failure) {
                rollbackQuietly(connection);
                throw failure;
            } catch (SQLException failure) {
                rollbackQuietly(connection);
                return false;
            } finally {
                restoreAutoCommitQuietly(connection);
            }
        } catch (SQLException failure) {
            return false;
        }
    }

    public boolean deleteAvailable(UUID graveUuid) {
        Objects.requireNonNull(graveUuid, "graveUuid");
        long now = clock.millis();
        if (!ensureClaimRow(graveUuid, now)) return false;

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (markAvailableDeleted(connection, graveUuid, now) != 1) {
                    connection.rollback();
                    return false;
                }
                int deleted;
                try (PreparedStatement statement = connection.prepareStatement(
                        "DELETE FROM " + graveTable + " WHERE uuid = ?")) {
                    statement.setString(1, graveUuid.toString());
                    deleted = statement.executeUpdate();
                }
                connection.commit();
                return deleted == 1;
            } catch (RuntimeException failure) {
                rollbackQuietly(connection);
                return false;
            } catch (Error failure) {
                rollbackQuietly(connection);
                throw failure;
            } catch (SQLException failure) {
                rollbackQuietly(connection);
                return false;
            } finally {
                restoreAutoCommitQuietly(connection);
            }
        } catch (SQLException failure) {
            return false;
        }
    }

    public boolean commitInventory(Claim claim, String serializedInventory, int experience) {
        Objects.requireNonNull(claim, "claim");
        Objects.requireNonNull(serializedInventory, "serializedInventory");

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                int claimChanged;
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE " + claimTable + " SET state = 'AVAILABLE', "
                                + "claim_token = NULL, claimant_uuid = NULL, server_id = NULL, "
                                + "operation = NULL, claimed_at = NULL, updated_at = ?, reason = NULL "
                                + "WHERE grave_uuid = ? AND state = 'ACTIVE' AND claim_token = ?")) {
                    statement.setLong(1, clock.millis());
                    statement.setString(2, claim.graveUuid().toString());
                    statement.setString(3, claim.token().toString());
                    claimChanged = statement.executeUpdate();
                }

                if (claimChanged != 1) {
                    connection.rollback();
                    return false;
                }

                int graveChanged;
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE " + graveTable
                                + " SET inventory = ?, experience = ? WHERE uuid = ?")) {
                    statement.setString(1, serializedInventory);
                    statement.setInt(2, experience);
                    statement.setString(3, claim.graveUuid().toString());
                    graveChanged = statement.executeUpdate();
                }

                if (graveChanged != 1) {
                    connection.rollback();
                    return false;
                }
                connection.commit();
                return true;
            } catch (RuntimeException failure) {
                rollbackQuietly(connection);
                return false;
            } catch (Error failure) {
                rollbackQuietly(connection);
                throw failure;
            } catch (SQLException failure) {
                rollbackQuietly(connection);
                return false;
            } finally {
                restoreAutoCommitQuietly(connection);
            }
        } catch (SQLException failure) {
            return false;
        }
    }

    public boolean completeDeletion(Claim claim) {
        Objects.requireNonNull(claim, "claim");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                int claimChanged;
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE " + claimTable
                                + " SET state = 'DELETED', claim_token = NULL, claimant_uuid = NULL, "
                                + "server_id = NULL, operation = NULL, claimed_at = NULL, "
                                + "updated_at = ?, reason = NULL "
                                + "WHERE grave_uuid = ? AND state = 'ACTIVE' AND claim_token = ?")) {
                    statement.setLong(1, clock.millis());
                    statement.setString(2, claim.graveUuid().toString());
                    statement.setString(3, claim.token().toString());
                    claimChanged = statement.executeUpdate();
                }

                int graveChanged;
                try (PreparedStatement statement = connection.prepareStatement(
                        "DELETE FROM " + graveTable + " WHERE uuid = ?")) {
                    statement.setString(1, claim.graveUuid().toString());
                    graveChanged = statement.executeUpdate();
                }

                if (graveChanged != 1 || claimChanged != 1) {
                    connection.rollback();
                    return false;
                }
                connection.commit();
                return true;
            } catch (RuntimeException failure) {
                rollbackQuietly(connection);
                return false;
            } catch (Error failure) {
                rollbackQuietly(connection);
                throw failure;
            } catch (SQLException failure) {
                rollbackQuietly(connection);
                return false;
            } finally {
                restoreAutoCommitQuietly(connection);
            }
        } catch (SQLException failure) {
            return false;
        }
    }

    public boolean quarantine(Claim claim, String reason) {
        Objects.requireNonNull(claim, "claim");
        String safeReason = reason == null ? "ambiguous-operation"
                : reason.substring(0, Math.min(reason.length(), 255));
        String sql = "UPDATE " + claimTable + " SET state = 'QUARANTINED', "
                + "updated_at = ?, reason = ? WHERE grave_uuid = ? "
                + "AND state = 'ACTIVE' AND claim_token = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, clock.millis());
            statement.setString(2, safeReason);
            statement.setString(3, claim.graveUuid().toString());
            statement.setString(4, claim.token().toString());
            return statement.executeUpdate() == 1;
        } catch (SQLException failure) {
            return false;
        }
    }

    public int quarantineActiveFromServer(String serverId, String reason) throws SQLException {
        Objects.requireNonNull(serverId, "serverId");
        String safeReason = reason == null ? "unclean-restart"
                : reason.substring(0, Math.min(reason.length(), 255));
        String select = "SELECT grave_uuid, claim_token, server_id FROM " + claimTable
                + " WHERE state = 'ACTIVE'";
        String update = "UPDATE " + claimTable + " SET state = 'QUARANTINED', "
                + "updated_at = ?, reason = ? WHERE grave_uuid = ? "
                + "AND state = 'ACTIVE' AND claim_token = ?";
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                java.util.List<String[]> matches = new java.util.ArrayList<>();
                try (PreparedStatement statement = connection.prepareStatement(select);
                     ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        if (serverId.equals(rows.getString("server_id"))) {
                            matches.add(new String[]{
                                    rows.getString("grave_uuid"), rows.getString("claim_token")});
                        }
                    }
                }

                int quarantined = 0;
                try (PreparedStatement statement = connection.prepareStatement(update)) {
                    for (String[] match : matches) {
                        statement.setLong(1, clock.millis());
                        statement.setString(2, safeReason);
                        statement.setString(3, match[0]);
                        statement.setString(4, match[1]);
                        quarantined += statement.executeUpdate();
                    }
                }
                connection.commit();
                return quarantined;
            } catch (SQLException | RuntimeException failure) {
                rollbackQuietly(connection);
                throw failure;
            } finally {
                restoreAutoCommitQuietly(connection);
            }
        }
    }

    private Payload readPayload(Connection connection, UUID graveUuid) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT inventory, equipment, experience FROM " + graveTable + " WHERE uuid = ?")) {
            statement.setString(1, graveUuid.toString());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) return null;
                return new Payload(rows.getString("inventory"), rows.getString("equipment"),
                        rows.getInt("experience"));
            }
        }
    }

    private boolean graveExists(Connection connection, UUID graveUuid) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM " + graveTable + " WHERE uuid = ?")) {
            statement.setString(1, graveUuid.toString());
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next();
            }
        }
    }

    private int lockAvailable(Connection connection, UUID graveUuid, long now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE " + claimTable + " SET updated_at = ? "
                        + "WHERE grave_uuid = ? AND state = 'AVAILABLE'")) {
            statement.setLong(1, now);
            statement.setString(2, graveUuid.toString());
            return statement.executeUpdate();
        }
    }

    private int markAvailableDeleted(Connection connection, UUID graveUuid, long now) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE " + claimTable
                        + " SET state = 'DELETED', claim_token = NULL, claimant_uuid = NULL, "
                        + "server_id = NULL, operation = NULL, claimed_at = NULL, updated_at = ?, reason = NULL "
                        + "WHERE grave_uuid = ? AND state = 'AVAILABLE'")) {
            statement.setLong(1, now);
            statement.setString(2, graveUuid.toString());
            return statement.executeUpdate();
        }
    }

    private int activateAvailable(Connection connection, Claim claim, long now) throws SQLException {
        String sql = "UPDATE " + claimTable + " SET state = 'ACTIVE', claim_token = ?, "
                + "claimant_uuid = ?, server_id = ?, operation = ?, claimed_at = ?, "
                + "updated_at = ?, reason = NULL WHERE grave_uuid = ? AND state = 'AVAILABLE'";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindClaim(statement, claim, now, 1);
            statement.setString(7, claim.graveUuid().toString());
            return statement.executeUpdate();
        }
    }

    private boolean ensureClaimRow(UUID graveUuid, long now) {
        String select = "SELECT 1 FROM " + claimTable + " WHERE grave_uuid = ?";
        String insert = "INSERT INTO " + claimTable
                + " (grave_uuid, state, updated_at) VALUES (?, 'AVAILABLE', ?)";
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(select)) {
                statement.setString(1, graveUuid.toString());
                try (ResultSet rows = statement.executeQuery()) {
                    if (rows.next()) return true;
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(insert)) {
                statement.setString(1, graveUuid.toString());
                statement.setLong(2, now);
                statement.executeUpdate();
                return true;
            }
        } catch (SQLException contentionOrFailure) {
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(select)) {
                statement.setString(1, graveUuid.toString());
                try (ResultSet rows = statement.executeQuery()) {
                    return rows.next();
                }
            } catch (SQLException failure) {
                return false;
            }
        }
    }

    private int markDeleted(Connection connection, Claim claim, long now) throws SQLException {
        String sql = "UPDATE " + claimTable
                + " SET state = 'DELETED', claim_token = NULL, claimant_uuid = NULL, "
                + "server_id = NULL, operation = NULL, claimed_at = NULL, updated_at = ?, reason = NULL "
                + "WHERE grave_uuid = ? AND state = 'ACTIVE' AND claim_token = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, now);
            statement.setString(2, claim.graveUuid().toString());
            statement.setString(3, claim.token().toString());
            return statement.executeUpdate();
        }
    }

    private void bindClaim(PreparedStatement statement, Claim claim, long now, int offset)
            throws SQLException {
        statement.setString(offset, claim.token().toString());
        statement.setString(offset + 1,
                claim.claimantUuid() == null ? null : claim.claimantUuid().toString());
        statement.setString(offset + 2, claim.serverId());
        statement.setString(offset + 3, claim.operation().name());
        statement.setLong(offset + 4, now);
        statement.setLong(offset + 5, now);
    }

    private Denied readDenial(Connection connection, UUID graveUuid) throws SQLException {
        String sql = "SELECT state, server_id, claimant_uuid FROM " + claimTable
                + " WHERE grave_uuid = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, graveUuid.toString());
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) return null;
                State state = State.valueOf(rows.getString("state"));
                if (state == State.DELETED) {
                    return new Denied(Denial.NOT_FOUND, null, null);
                }
                Denial reason = state == State.QUARANTINED
                        ? Denial.QUARANTINED : Denial.ACTIVE;
                String claimant = rows.getString("claimant_uuid");
                return new Denied(reason, rows.getString("server_id"),
                        claimant == null ? null : UUID.fromString(claimant));
            }
        }
    }

    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
        }
    }

    private static void restoreAutoCommitQuietly(Connection connection) {
        try {
            connection.setAutoCommit(true);
        } catch (SQLException ignored) {
        }
    }
}
