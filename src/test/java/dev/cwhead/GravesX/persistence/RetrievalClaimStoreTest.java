package dev.cwhead.GravesX.persistence;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetrievalClaimStoreTest {
    private DataSource dataSource;
    private RetrievalClaimStore store;

    @BeforeEach
    void setUp() throws Exception {
        JdbcDataSource h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:claims-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource = h2;
        store = new RetrievalClaimStore(dataSource, "graves_");

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE graves_grave (uuid VARCHAR(36) PRIMARY KEY, inventory CLOB, equipment CLOB, experience INT, protection INT, provider_id VARCHAR(255))");
        }
        store.createTable();
    }

    @Test
    void acquisitionReturnsAuthoritativePayloadFromClaimTransaction() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "sql-items", 17);

        RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.BREAK_DROP));

        assertEquals("sql-items", acquired.payload().inventory());
        assertEquals(17, acquired.payload().experience());
    }

    @Test
    void exactlyOneConcurrentClaimWins() throws Exception {
        UUID graveId = UUID.randomUUID();
        try (Connection connection = dataSource.getConnection();
             var insert = connection.prepareStatement(
                     "INSERT INTO graves_grave (uuid, inventory) VALUES (?, 'items')")) {
            insert.setString(1, graveId.toString());
            insert.executeUpdate();
        }

        int contenders = 16;
        ExecutorService executor = Executors.newFixedThreadPool(contenders);
        CountDownLatch ready = new CountDownLatch(contenders);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<RetrievalClaimStore.AcquireResult>> futures = new ArrayList<>();

        try {
            for (int index = 0; index < contenders; index++) {
                UUID claimant = UUID.randomUUID();
                Callable<RetrievalClaimStore.AcquireResult> task = () -> {
                    ready.countDown();
                    start.await();
                    return store.acquire(graveId, claimant, "server-a",
                            RetrievalClaimStore.Operation.AUTO_LOOT);
                };
                futures.add(executor.submit(task));
            }
            ready.await();
            start.countDown();

            long acquired = 0;
            long deniedActive = 0;
            for (Future<RetrievalClaimStore.AcquireResult> future : futures) {
                RetrievalClaimStore.AcquireResult result = future.get();
                if (result instanceof RetrievalClaimStore.Acquired) {
                    acquired++;
                } else if (result instanceof RetrievalClaimStore.Denied denied
                        && denied.reason() == RetrievalClaimStore.Denial.ACTIVE) {
                    deniedActive++;
                }
            }

            assertEquals(1, acquired);
            assertEquals(contenders - 1, deniedActive);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void inventoryAndClaimReleaseCommitAtomically() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "before", 17);

        RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.INVENTORY_OPEN));

        assertTrue(store.commitInventory(acquired.claim(), "after", 0));

        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "SELECT g.inventory, g.experience, c.state FROM graves_grave g "
                             + "JOIN graves_grave_claim c ON c.grave_uuid = g.uuid WHERE g.uuid = ?")) {
            statement.setString(1, graveId.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                assertEquals("after", rows.getString("inventory"));
                assertEquals(0, rows.getInt("experience"));
                assertEquals("AVAILABLE", rows.getString("state"));
            }
        }
    }

    @Test
    void graveDeletionLeavesAtomicTombstone() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "items", 7);
        RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.AUTO_LOOT));

        assertTrue(store.completeDeletion(acquired.claim()));

        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "SELECT (SELECT COUNT(*) FROM graves_grave WHERE uuid = ?) AS graves, "
                             + "(SELECT state FROM graves_grave_claim WHERE grave_uuid = ?) AS claim_state")) {
            statement.setString(1, graveId.toString());
            statement.setString(2, graveId.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                assertEquals(0, rows.getInt("graves"));
                assertEquals("DELETED", rows.getString("claim_state"));
            }
        }
    }

    @Test
    void concurrentAcquireCannotCreateOrphanAfterDeletion() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "items", 7);
        RetrievalClaimStore.Acquired deleting = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.DELETE));

        CountDownLatch beforeInsert = new CountDownLatch(1);
        CountDownLatch allowInsert = new CountDownLatch(1);
        RetrievalClaimStore contenderStore = new RetrievalClaimStore(
                dataSource, "graves_", Clock.systemUTC(), () -> {
            beforeInsert.countDown();
            try {
                if (!allowInsert.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("timed out waiting to test acquire/delete race");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<RetrievalClaimStore.AcquireResult> contender = executor.submit(() ->
                    contenderStore.acquire(graveId, UUID.randomUUID(), "server-b",
                            RetrievalClaimStore.Operation.AUTO_LOOT));
            assertTrue(beforeInsert.await(5, TimeUnit.SECONDS));
            assertTrue(store.completeDeletion(deleting.claim()));
            allowInsert.countDown();

            RetrievalClaimStore.Denied denied = assertInstanceOf(
                    RetrievalClaimStore.Denied.class, contender.get(5, TimeUnit.SECONDS));
            assertEquals(RetrievalClaimStore.Denial.NOT_FOUND, denied.reason());
            try (Connection connection = dataSource.getConnection();
                 var statement = connection.prepareStatement(
                         "SELECT state FROM graves_grave_claim WHERE grave_uuid = ?")) {
                statement.setString(1, graveId.toString());
                try (var rows = statement.executeQuery()) {
                    assertTrue(rows.next());
                    assertEquals("DELETED", rows.getString("state"));
                    assertFalse(rows.next());
                }
            }
        } finally {
            allowInsert.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void quarantinedClaimCannotBeAcquiredAgain() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "items", 7);
        RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.AUTO_LOOT));

        assertTrue(store.quarantine(acquired.claim(), "ambiguous-player-save"));

        RetrievalClaimStore.Denied denied = assertInstanceOf(
                RetrievalClaimStore.Denied.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.AUTO_LOOT));
        assertEquals(RetrievalClaimStore.Denial.QUARANTINED, denied.reason());
    }

    @Test
    void restartQuarantinesOnlyClaimsFromSameServer() throws Exception {
        UUID graveA = UUID.randomUUID();
        UUID graveB = UUID.randomUUID();
        insertGrave(graveA, "a", 0);
        insertGrave(graveB, "b", 0);
        assertInstanceOf(RetrievalClaimStore.Acquired.class,
                store.acquire(graveA, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.INVENTORY_OPEN));
        assertInstanceOf(RetrievalClaimStore.Acquired.class,
                store.acquire(graveB, UUID.randomUUID(), "server-b",
                        RetrievalClaimStore.Operation.INVENTORY_OPEN));

        assertEquals(1, store.quarantineActiveFromServer("server-a", "unclean-restart"));

        RetrievalClaimStore.Denied deniedA = assertInstanceOf(
                RetrievalClaimStore.Denied.class,
                store.acquire(graveA, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.INVENTORY_OPEN));
        RetrievalClaimStore.Denied deniedB = assertInstanceOf(
                RetrievalClaimStore.Denied.class,
                store.acquire(graveB, UUID.randomUUID(), "server-b",
                        RetrievalClaimStore.Operation.INVENTORY_OPEN));
        assertEquals(RetrievalClaimStore.Denial.QUARANTINED, deniedA.reason());
        assertEquals(RetrievalClaimStore.Denial.ACTIVE, deniedB.reason());
    }

    @Test
    void restartServerIdMatchIsCaseSensitiveOnCaseInsensitiveDatabase() throws Exception {
        JdbcDataSource caseInsensitive = new JdbcDataSource();
        caseInsensitive.setURL("jdbc:h2:mem:claims-case-" + UUID.randomUUID()
                + ";DB_CLOSE_DELAY=-1;IGNORECASE=TRUE");
        RetrievalClaimStore caseStore = new RetrievalClaimStore(caseInsensitive, "graves_");
        UUID graveId = UUID.randomUUID();
        try (Connection connection = caseInsensitive.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE graves_grave (uuid VARCHAR(36) PRIMARY KEY, inventory CLOB, equipment CLOB, experience INT, protection INT, provider_id VARCHAR(255))");
        }
        caseStore.createTable();
        try (Connection connection = caseInsensitive.getConnection();
             var insert = connection.prepareStatement(
                     "INSERT INTO graves_grave (uuid, inventory, experience) VALUES (?, 'items', 0)")) {
            insert.setString(1, graveId.toString());
            insert.executeUpdate();
        }
        assertInstanceOf(RetrievalClaimStore.Acquired.class,
                caseStore.acquire(graveId, UUID.randomUUID(), "SERVER-A",
                        RetrievalClaimStore.Operation.INVENTORY_OPEN));

        assertEquals(0, caseStore.quarantineActiveFromServer("server-a", "unclean-restart"));
        RetrievalClaimStore.Denied denied = assertInstanceOf(
                RetrievalClaimStore.Denied.class,
                caseStore.acquire(graveId, UUID.randomUUID(), "SERVER-A",
                        RetrievalClaimStore.Operation.INVENTORY_OPEN));
        assertEquals(RetrievalClaimStore.Denial.ACTIVE, denied.reason());
    }

    @Test
    void wrongTokenCannotCommitInventory() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "before", 9);
        RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.INVENTORY_OPEN));
        RetrievalClaimStore.Claim wrongToken = new RetrievalClaimStore.Claim(
                graveId, UUID.randomUUID(), acquired.claim().claimantUuid(),
                acquired.claim().serverId(), acquired.claim().operation());

        assertFalse(store.commitInventory(wrongToken, "tampered", 0));

        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "SELECT g.inventory, g.experience, c.state FROM graves_grave g "
                             + "JOIN graves_grave_claim c ON c.grave_uuid = g.uuid WHERE g.uuid = ?")) {
            statement.setString(1, graveId.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                assertEquals("before", rows.getString("inventory"));
                assertEquals(9, rows.getInt("experience"));
                assertEquals("ACTIVE", rows.getString("state"));
            }
        }
    }

    @Test
    void wrongTokenCommitDoesNotTouchLockedGraveRow() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "before", 9);
        RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.INVENTORY_OPEN));

        try (Connection lockingConnection = dataSource.getConnection();
             var lock = lockingConnection.prepareStatement(
                     "SELECT uuid FROM graves_grave WHERE uuid = ? FOR UPDATE")) {
            lockingConnection.setAutoCommit(false);
            lock.setString(1, graveId.toString());
            try (var rows = lock.executeQuery()) {
                assertTrue(rows.next());
            }

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<Boolean> rejected = executor.submit(() ->
                        store.commitInventory(withWrongToken(acquired.claim()), "tampered", 0));
                assertFalse(rejected.get(1, TimeUnit.SECONDS));
            } finally {
                executor.shutdownNow();
                lockingConnection.rollback();
            }
        }
    }

    @Test
    void concurrentCommitAndDeletionHaveOneConsistentWinner() throws Exception {
        for (int iteration = 0; iteration < 25; iteration++) {
            UUID graveId = UUID.randomUUID();
            insertGrave(graveId, "before", 9);
            RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                    RetrievalClaimStore.Acquired.class,
                    store.acquire(graveId, UUID.randomUUID(), "server-a",
                            RetrievalClaimStore.Operation.INVENTORY_OPEN));

            ExecutorService executor = Executors.newFixedThreadPool(2);
            CountDownLatch start = new CountDownLatch(1);
            try {
                Future<Boolean> commit = executor.submit(() -> {
                    start.await();
                    return store.commitInventory(acquired.claim(), "after", 0);
                });
                Future<Boolean> deletion = executor.submit(() -> {
                    start.await();
                    return store.completeDeletion(acquired.claim());
                });
                start.countDown();

                boolean commitWon = commit.get(5, TimeUnit.SECONDS);
                boolean deletionWon = deletion.get(5, TimeUnit.SECONDS);
                assertTrue(commitWon ^ deletionWon);

                try (Connection connection = dataSource.getConnection();
                     var statement = connection.prepareStatement(
                             "SELECT c.state, g.inventory FROM graves_grave_claim c "
                                     + "LEFT JOIN graves_grave g ON g.uuid = c.grave_uuid "
                                     + "WHERE c.grave_uuid = ?")) {
                    statement.setString(1, graveId.toString());
                    try (var rows = statement.executeQuery()) {
                        assertTrue(rows.next());
                        if (commitWon) {
                            assertEquals("AVAILABLE", rows.getString("state"));
                            assertEquals("after", rows.getString("inventory"));
                        } else {
                            assertEquals("DELETED", rows.getString("state"));
                            assertEquals(null, rows.getString("inventory"));
                        }
                    }
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void wrongTokenCannotDeleteGrave() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "before", 9);
        RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.DELETE));

        assertFalse(store.completeDeletion(withWrongToken(acquired.claim())));

        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "SELECT g.inventory, c.state FROM graves_grave g "
                             + "JOIN graves_grave_claim c ON c.grave_uuid = g.uuid WHERE g.uuid = ?")) {
            statement.setString(1, graveId.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                assertEquals("before", rows.getString("inventory"));
                assertEquals("ACTIVE", rows.getString("state"));
            }
        }
    }

    @Test
    void wrongTokenCannotQuarantineClaim() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "before", 9);
        RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.AUTO_LOOT));

        assertFalse(store.quarantine(withWrongToken(acquired.claim()), "tampered"));

        RetrievalClaimStore.Denied denied = assertInstanceOf(
                RetrievalClaimStore.Denied.class,
                store.acquire(graveId, UUID.randomUUID(), "server-b",
                        RetrievalClaimStore.Operation.AUTO_LOOT));
        assertEquals(RetrievalClaimStore.Denial.ACTIVE, denied.reason());
    }

    private RetrievalClaimStore.Claim withWrongToken(RetrievalClaimStore.Claim claim) {
        return new RetrievalClaimStore.Claim(claim.graveUuid(), UUID.randomUUID(),
                claim.claimantUuid(), claim.serverId(), claim.operation());
    }

    @Test
    void availablePayloadUpdateIsRejectedBeforeTouchingAnActiveGrave() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "before", 9);
        RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.INVENTORY_OPEN));

        try (Connection blocker = dataSource.getConnection();
             var lock = blocker.prepareStatement(
                     "UPDATE graves_grave SET inventory = inventory WHERE uuid = ?")) {
            blocker.setAutoCommit(false);
            lock.setString(1, graveId.toString());
            assertEquals(1, lock.executeUpdate());

            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                Future<Boolean> rejected = executor.submit(() ->
                        store.updateAvailableInventory(graveId, "forged", 0));
                assertFalse(rejected.get(1, TimeUnit.SECONDS));
            } finally {
                executor.shutdownNow();
                blocker.rollback();
            }
        }

        assertTrue(store.commitInventory(acquired.claim(), "claimed", 0));
        assertEquals("claimed", readInventory(graveId));
    }

    @Test
    void availablePayloadUpdateSucceedsOnlyWithoutAnActiveClaim() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "before", 9);

        assertTrue(store.updateAvailableInventory(graveId, "after", 4));
        assertEquals("after", readInventory(graveId));

        RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.INVENTORY_OPEN));
        assertFalse(store.updateAvailableInventory(graveId, "blocked", 1));
        assertTrue(store.quarantine(acquired.claim(), "ambiguous"));
        assertFalse(store.updateAvailableInventory(graveId, "blocked-again", 2));
        assertEquals("after", readInventory(graveId));
    }

    @Test
    void availableDeletionCreatesTombstoneAndRejectsActiveOrQuarantinedClaims() throws Exception {
        UUID available = UUID.randomUUID();
        insertGrave(available, "items", 1);
        assertTrue(store.deleteAvailable(available));
        assertGraveCountAndState(available, 0, "DELETED");

        UUID active = UUID.randomUUID();
        insertGrave(active, "items", 1);
        RetrievalClaimStore.Acquired claim = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(active, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.DELETE));
        assertFalse(store.deleteAvailable(active));
        assertTrue(store.quarantine(claim.claim(), "ambiguous"));
        assertFalse(store.deleteAvailable(active));
        assertGraveCountAndState(active, 1, "QUARANTINED");
    }

    @Test
    void allowlistedColumnUpdateRequiresAvailableState() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "items", 1);

        assertTrue(store.updateAvailableColumn(graveId, "protection", 1));
        assertEquals(1, readIntColumn(graveId, "protection"));
        assertThrows(IllegalArgumentException.class,
                () -> store.updateAvailableColumn(graveId, "uuid = NULL WHERE 1=1 --", 0));

        RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.ADDON_MUTATION));
        assertFalse(store.updateAvailableColumn(graveId, "protection", 0));
        assertEquals(1, readIntColumn(graveId, "protection"));
        assertTrue(store.quarantine(acquired.claim(), "ambiguous"));
        assertFalse(store.updateAvailableColumn(graveId, "provider_id", "provider"));
    }

    @Test
    void arbitraryFullRowMutationRunsOnlyInsideAvailableClaimTransaction() throws Exception {
        UUID graveId = UUID.randomUUID();
        insertGrave(graveId, "before", 3);

        assertTrue(store.mutateAvailable(graveId, connection -> {
            try (var statement = connection.prepareStatement(
                    "UPDATE graves_grave SET inventory = ?, experience = ? WHERE uuid = ?")) {
                statement.setString(1, "after");
                statement.setInt(2, 2);
                statement.setString(3, graveId.toString());
                return statement.executeUpdate() == 1;
            }
        }));
        assertEquals("after", readInventory(graveId));

        RetrievalClaimStore.Acquired acquired = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(graveId, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.ADDON_MUTATION));
        assertFalse(store.mutateAvailable(graveId, connection -> {
            throw new AssertionError("mutation callback must not run while claim is active");
        }));
        assertTrue(store.quarantine(acquired.claim(), "ambiguous"));
        assertFalse(store.mutateAvailable(graveId, connection -> true));
    }

    @Test
    void creationAtomicallyCreatesAvailableClaimAndRejectsTombstoneReuse() throws Exception {
        UUID graveId = UUID.randomUUID();
        assertTrue(store.createAvailable(graveId, connection -> {
            try (var statement = connection.prepareStatement(
                    "INSERT INTO graves_grave (uuid, inventory, experience) VALUES (?, ?, ?)")) {
                statement.setString(1, graveId.toString());
                statement.setString(2, "items");
                statement.setInt(3, 5);
                return statement.executeUpdate() == 1;
            }
        }));
        assertGraveCountAndState(graveId, 1, "AVAILABLE");
        assertTrue(store.deleteAvailable(graveId));
        assertFalse(store.createAvailable(graveId, connection -> {
            throw new AssertionError("tombstoned UUID must reject creation before payload insert");
        }));
        assertGraveCountAndState(graveId, 0, "DELETED");
    }

    @Test
    void backfillCreatesOnlyMissingAvailableRows() throws Exception {
        UUID legacy = UUID.randomUUID();
        insertGrave(legacy, "legacy", 1);
        UUID active = UUID.randomUUID();
        insertGrave(active, "active", 1);
        RetrievalClaimStore.Acquired claim = assertInstanceOf(
                RetrievalClaimStore.Acquired.class,
                store.acquire(active, UUID.randomUUID(), "server-a",
                        RetrievalClaimStore.Operation.INVENTORY_OPEN));

        assertEquals(1, store.backfillAvailableClaims());
        assertGraveCountAndState(legacy, 1, "AVAILABLE");
        assertGraveCountAndState(active, 1, "ACTIVE");
        assertEquals(0, store.backfillAvailableClaims());
        assertTrue(store.quarantine(claim.claim(), "ambiguous"));
    }

    @Test
    void callbackRuntimeFailuresRollbackBeforeConnectionReturn() throws Exception {
        UUID createId = UUID.randomUUID();
        assertFalse(store.createAvailable(createId, connection -> {
            throw new IllegalStateException("boom");
        }));
        assertRowCounts(createId, 0, 0);

        UUID mutateId = UUID.randomUUID();
        insertGrave(mutateId, "items", 1);
        assertFalse(store.mutateAvailable(mutateId, connection -> {
            try (var statement = connection.prepareStatement(
                    "UPDATE graves_grave SET inventory = 'changed' WHERE uuid = ?")) {
                statement.setString(1, mutateId.toString());
                statement.executeUpdate();
            }
            throw new IllegalStateException("boom");
        }));
        assertEquals("items", readInventory(mutateId));
        assertGraveCountAndState(mutateId, 1, "AVAILABLE");
    }

    @Test
    void creationAndMutationCallbacksMustPreserveRequestedPayloadRow() throws Exception {
        UUID missing = UUID.randomUUID();
        assertFalse(store.createAvailable(missing, connection -> true));
        assertRowCounts(missing, 0, 0);

        UUID requested = UUID.randomUUID();
        UUID wrong = UUID.randomUUID();
        assertFalse(store.createAvailable(requested, connection -> {
            try (var statement = connection.prepareStatement(
                    "INSERT INTO graves_grave (uuid, inventory, experience) VALUES (?, 'wrong', 0)")) {
                statement.setString(1, wrong.toString());
                return statement.executeUpdate() == 1;
            }
        }));
        assertRowCounts(requested, 0, 0);
        assertRowCounts(wrong, 0, 0);

        UUID existing = UUID.randomUUID();
        insertGrave(existing, "items", 1);
        assertFalse(store.mutateAvailable(existing, connection -> {
            try (var statement = connection.prepareStatement(
                    "DELETE FROM graves_grave WHERE uuid = ?")) {
                statement.setString(1, existing.toString());
                statement.executeUpdate();
                return true;
            }
        }));
        assertRowCounts(existing, 1, 1);
    }

    private void assertRowCounts(UUID graveId, int graves, int claims) throws Exception {
        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "SELECT (SELECT COUNT(*) FROM graves_grave WHERE uuid = ?) AS graves, "
                             + "(SELECT COUNT(*) FROM graves_grave_claim WHERE grave_uuid = ?) AS claims")) {
            statement.setString(1, graveId.toString());
            statement.setString(2, graveId.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                assertEquals(graves, rows.getInt("graves"));
                assertEquals(claims, rows.getInt("claims"));
            }
        }
    }

    private int readIntColumn(UUID graveId, String column) throws Exception {
        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "SELECT " + column + " FROM graves_grave WHERE uuid = ?")) {
            statement.setString(1, graveId.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                return rows.getInt(1);
            }
        }
    }

    private String readInventory(UUID graveId) throws Exception {
        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "SELECT inventory FROM graves_grave WHERE uuid = ?")) {
            statement.setString(1, graveId.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                return rows.getString(1);
            }
        }
    }

    private void assertGraveCountAndState(UUID graveId, int graves, String state) throws Exception {
        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "SELECT (SELECT COUNT(*) FROM graves_grave WHERE uuid = ?) AS graves, "
                             + "(SELECT state FROM graves_grave_claim WHERE grave_uuid = ?) AS state")) {
            statement.setString(1, graveId.toString());
            statement.setString(2, graveId.toString());
            try (var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                assertEquals(graves, rows.getInt("graves"));
                assertEquals(state, rows.getString("state"));
            }
        }
    }

    private void insertGrave(UUID graveId, String inventory, int experience) throws Exception {
        try (Connection connection = dataSource.getConnection();
             var insert = connection.prepareStatement(
                     "INSERT INTO graves_grave (uuid, inventory, experience) VALUES (?, ?, ?)")) {
            insert.setString(1, graveId.toString());
            insert.setString(2, inventory);
            insert.setInt(3, experience);
            insert.executeUpdate();
        }
    }
}
