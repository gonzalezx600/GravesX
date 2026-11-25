package dev.cwhead.GravesX.persistence;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersistenceGatewayIntegrationSourceTest {
    @Test
    void destructivePayloadEffectsRequireClaims() throws IOException {
        String source = Files.readString(Path.of(System.getProperty("user.dir"),
                "src/main/java/com/ranull/graves/manager/GraveManager.java"));

        String timeout = between(source, "private void handleGraveTimeout", "private boolean handleGraveAbandoned");
        assertTrue(timeout.contains("runClaimedDropAndDelete(grave, loc, RetrievalClaimStore.Operation.TIMEOUT_DROP)"));
        assertFalse(timeout.contains("dropGraveItems(loc, grave)"));
        assertFalse(timeout.contains("dropGraveExperience(loc, grave)"));

        String oldest = between(source, "public void removeOldestGrave", "public void dropAllFromGrave");
        assertTrue(oldest.contains("runClaimedDropAndDelete(toDel, deathLoc, RetrievalClaimStore.Operation.TIMEOUT_DROP)"));
        assertTrue(oldest.contains("failClosedLegacyPayloadEffect(\"remove-oldest-invalid-location\""));
        assertFalse(oldest.contains("dropGraveItems(deathLoc, toDel)"));

        String broken = between(source, "public void breakGrave(Location location, Grave grave)", "public void dropGraveItems");
        assertTrue(broken.contains("runClaimedDropAndDelete(grave, location, RetrievalClaimStore.Operation.BREAK_DROP)"));
        String claimedDrop = between(source, "private void runClaimedDropAndDelete", "public void breakGrave(Grave grave)");
        assertTrue(claimedDrop.contains("acquired.payload()"));
        assertTrue(claimedDrop.contains("RetrievalClaimStore.Payload payload"));
        assertTrue(broken.contains("failClosedLegacyPayloadEffect(\"break-invalid-location\""));
        assertFalse(broken.contains("dropGraveItems(anchor, grave)"));

        String legacyDrop = between(source, "public void dropGraveItems", "public void giveGraveExperience");
        assertTrue(legacyDrop.contains("failClosedLegacyPayloadEffect"));
        assertFalse(legacyDrop.contains("dropItemNaturally"));

        String legacyExperience = between(source, "public void dropGraveExperience", "public List<Grave> getGraveList(Player player)");
        assertTrue(legacyExperience.contains("failClosedLegacyPayloadEffect"));
        assertFalse(legacyExperience.contains("spawnEntity"));

        String open = between(source, "public boolean openGrave(Entity entity, Location location, Grave grave, boolean preview)", "public void autoLootGrave");
        assertTrue(open.contains("if (!isClaimedInventoryOpenEnabled())"));
        assertTrue(open.indexOf("failClosedLegacyPayloadEffect") < open.indexOf("openLocalizedGraveInventory"));

        String autoloot = between(source, "public void autoLootGrave", "private void syncBagOfGoldBalanceAfterAutoloot");
        assertTrue(autoloot.contains("if (!isClaimedInventoryOpenEnabled())"));
        assertTrue(autoloot.indexOf("failClosedLegacyPayloadEffect") < autoloot.indexOf("player.getInventory().setItem"));

        String dropAll = between(source, "public void dropAllFromGrave", "public void removeGrave(Grave grave)");
        assertTrue(dropAll.contains("failClosedLegacyPayloadEffect"));
        assertFalse(dropAll.contains("dropItemNaturally"));

        String inventoryApi = Files.readString(Path.of(System.getProperty("user.dir"),
                "src/main/java/dev/cwhead/GravesX/api/inventory/InventoryAPI.java"));
        String equipArmor = between(inventoryApi, "public void equipArmor", "public void equipItems");
        assertTrue(equipArmor.indexOf("refuseGravePayloadMutation") < equipArmor.indexOf("InventoryUtil.equipArmor"));
        String equipItems = between(inventoryApi, "public void equipItems", "public String inventoryToString");
        assertTrue(equipItems.indexOf("refuseGravePayloadMutation") < equipItems.indexOf("InventoryUtil.equipItems"));
    }

    @Test
    void allDataManagerGraveWritesUseClaimGateway() throws IOException {
        String source = Files.readString(Path.of(System.getProperty("user.dir"),
                "src/main/java/com/ranull/graves/manager/DataManager.java"));

        String add = between(source, "public void addGrave(Grave grave)", "private void bindStatementParameters");
        assertTrue(add.contains("retrievalClaimStore.createAvailable"));

        String write = between(source, "private boolean writeGraveRow", "public void removeGrave(Grave grave)");
        assertTrue(write.contains("retrievalClaimStore.mutateAvailable"));

        String remove = between(source, "public CompletionStage<Boolean> deleteGrave(UUID uuid)", "public void updateGrave(Grave grave, String column, int integer)");
        assertTrue(remove.contains("retrievalClaimStore.deleteAvailable"));
        assertFalse(remove.contains("DELETE FROM"));

        String graveManager = Files.readString(Path.of(System.getProperty("user.dir"),
                "src/main/java/com/ranull/graves/manager/GraveManager.java"));
        String cleanup = between(graveManager, "public void removeGrave(Grave grave)", "private void removeWithProvider");
        assertTrue(cleanup.contains("deleteGrave(grave.getUUID()).whenComplete"));
        assertTrue(cleanup.contains("plugin.getSchedulerManager().execute(finalAnchor, work)"));
        assertFalse(cleanup.contains("getDataManager().removeGrave"));
        assertTrue(source.contains("catch (RuntimeException | Error submissionFailure)"));
        assertTrue(cleanup.contains("catch (RuntimeException | Error schedulingFailure)"));

        String updates = between(source, "public void updateGrave(Grave grave, String column, int integer)", "public Grave resultSetToGrave");
        assertTrue(updates.contains("retrievalClaimStore.updateAvailableColumn"));
        assertFalse(updates.contains("UPDATE \" + getStoragePrefix() + \"grave SET"));
    }

    private static String between(String source, String start, String end) {
        int from = source.indexOf(start);
        int to = source.indexOf(end, from);
        assertTrue(from >= 0, "missing source start: " + start);
        assertTrue(to > from, "missing source end: " + end);
        return source.substring(from, to);
    }
}
