package dev.cwhead.GravesX.event;

/**
 * Centralizes the decision shared by modern and legacy cancellable grave events.
 */
public final class EventDecision {
    private EventDecision() {
    }

    public static boolean mayProceed(
            boolean modernCancelled,
            boolean modernAddon,
            boolean legacyCancelled,
            boolean legacyAddon) {
        return !(modernCancelled || modernAddon || legacyCancelled || legacyAddon);
    }
}
