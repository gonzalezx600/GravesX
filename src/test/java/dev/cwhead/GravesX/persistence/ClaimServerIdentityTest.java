package dev.cwhead.GravesX.persistence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClaimServerIdentityTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void identityPersistsAcrossRestarts() throws Exception {
        Path file = temporaryDirectory.resolve("claim-server-id");

        String first = ClaimServerIdentity.loadOrCreate(file);
        String second = ClaimServerIdentity.loadOrCreate(file);

        assertEquals(first, second);
        assertEquals(UUID.fromString(first).toString(), first);
        assertEquals(first + System.lineSeparator(), Files.readString(file));
    }

    @Test
    void separateServersReceiveSeparateIdentities() throws Exception {
        String first = ClaimServerIdentity.loadOrCreate(
                temporaryDirectory.resolve("server-a/claim-server-id"));
        String second = ClaimServerIdentity.loadOrCreate(
                temporaryDirectory.resolve("server-b/claim-server-id"));

        assertNotEquals(first, second);
    }

    @Test
    void malformedIdentityFailsClosedWithoutReplacement() throws Exception {
        Path file = temporaryDirectory.resolve("claim-server-id");
        Files.writeString(file, "not-a-uuid\n");

        assertThrows(IllegalStateException.class, () -> ClaimServerIdentity.loadOrCreate(file));
        assertEquals("not-a-uuid\n", Files.readString(file));
    }

    @Test
    void windowsSkipsUnsupportedDirectoryFsync() {
        assertDoesNotThrow(() -> ClaimServerIdentity.forceDirectoryForPlatform(
                temporaryDirectory.resolve("missing-directory"), "Windows 11"));
    }

    @Test
    void concurrentCreationReturnsOneStableIdentity() throws Exception {
        Path file = temporaryDirectory.resolve("claim-server-id");
        int contenders = 8;
        var executor = Executors.newFixedThreadPool(contenders);
        var ready = new CountDownLatch(contenders);
        var start = new CountDownLatch(1);
        try {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<String>>();
            for (int index = 0; index < contenders; index++) {
                Callable<String> task = () -> {
                    ready.countDown();
                    start.await();
                    return ClaimServerIdentity.loadOrCreate(file);
                };
                futures.add(executor.submit(task));
            }
            ready.await();
            start.countDown();
            String identity = futures.getFirst().get();
            for (var future : futures) {
                assertEquals(identity, future.get());
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
