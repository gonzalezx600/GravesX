package dev.cwhead.GravesX.event;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventDecisionTest {

    @Test
    void allowsActionWhenNeitherEventBlocksIt() {
        assertTrue(EventDecision.mayProceed(false, false, false, false));
    }

    @Test
    void blocksActionWhenModernEventIsCancelled() {
        assertFalse(EventDecision.mayProceed(true, false, false, false));
    }

    @Test
    void blocksActionWhenLegacyEventIsCancelled() {
        assertFalse(EventDecision.mayProceed(false, false, true, false));
    }

    @Test
    void blocksActionWhenModernEventIsAddonOwned() {
        assertFalse(EventDecision.mayProceed(false, true, false, false));
    }

    @Test
    void blocksActionWhenLegacyEventIsAddonOwned() {
        assertFalse(EventDecision.mayProceed(false, false, false, true));
    }
}
