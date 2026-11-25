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
            statement.execute("CREATE TABLE graves_grave (uuid VARCHAR(36) PRIMARY KEY, inventory CLOB, experience INT)");
        }
        store.createTable();
    }

    @Test
    void onlyOneConcurrentAcquireWins() throws Exception {
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
            statement.execute("CREATE TABLE graves_grave (uuid VARCHAR(36) PRIMARY KEY, inventory CLOB, experience INT)");
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
