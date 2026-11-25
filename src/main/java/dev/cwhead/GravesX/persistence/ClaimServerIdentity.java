package dev.cwhead.GravesX.persistence;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.UUID;

/** Loads the persistent identity used to recognize this server's abandoned claims. */
public final class ClaimServerIdentity {
    private ClaimServerIdentity() {
    }

    public static synchronized String loadOrCreate(Path identityFile) {
        Objects.requireNonNull(identityFile, "identityFile");
        if (Files.exists(identityFile)) {
            return readExisting(identityFile);
        }

        Path parent = identityFile.toAbsolutePath().getParent();
        try {
            if (parent != null) Files.createDirectories(parent);
            String identity = UUID.randomUUID().toString();
            byte[] bytes = (identity + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
            try (FileChannel channel = FileChannel.open(identityFile,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            forceDirectoryForPlatform(parent, System.getProperty("os.name", ""));
            return identity;
        } catch (java.nio.file.FileAlreadyExistsException raced) {
            return readExisting(identityFile);
        } catch (IOException failure) {
            throw new IllegalStateException("Unable to persist grave claim server identity", failure);
        }
    }

    static void forceDirectoryForPlatform(Path directory, String osName) throws IOException {
        if (directory == null || osName.toLowerCase(java.util.Locale.ROOT).contains("windows")) return;
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static String readExisting(Path identityFile) {
        try {
            String raw = Files.readString(identityFile, StandardCharsets.UTF_8).strip();
            String canonical = UUID.fromString(raw).toString();
            if (!canonical.equals(raw)) {
                throw new IllegalArgumentException("identity is not a canonical UUID");
            }
            return canonical;
        } catch (IOException | IllegalArgumentException failure) {
            throw new IllegalStateException(
                    "Invalid grave claim server identity at " + identityFile.toAbsolutePath(), failure);
        }
    }
}
