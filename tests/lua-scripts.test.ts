import { describe, it, expect } from "vitest";
import { CLAIM_LUA, RECOVER_STALLED_LUA, RENEW_LOCK_LUA } from "../src/lib/scripts/index.js";

describe("Lua scripts", () => {
  describe("CLAIM_LUA", () => {
    it("is a non-empty string", () => {
      expect(typeof CLAIM_LUA).toBe("string");
      expect(CLAIM_LUA.trim().length).toBeGreaterThan(0);
    });

    it("promotes delayed jobs before claiming", () => {
      expect(CLAIM_LUA).toContain("ZRANGEBYSCORE");
      expect(CLAIM_LUA).toContain("ZADD");
      expect(CLAIM_LUA).toContain("ZREM");
    });

    it("pops the highest-priority job with ZPOPMIN", () => {
      expect(CLAIM_LUA).toContain("ZPOPMIN");
    });

    it("locks the claimed job by setting status to active", () => {
      expect(CLAIM_LUA).toContain("'active'");
      expect(CLAIM_LUA).toContain("SADD");
      expect(CLAIM_LUA).toContain("lockId");
      expect(CLAIM_LUA).toContain("lockExpiresAt");
    });

    it("returns empty string when no job is available", () => {
      expect(CLAIM_LUA).toContain("return ''");
    });
  });

  describe("RECOVER_STALLED_LUA", () => {
    it("is a non-empty string", () => {
      expect(typeof RECOVER_STALLED_LUA).toBe("string");
      expect(RECOVER_STALLED_LUA.trim().length).toBeGreaterThan(0);
    });

    it("guards against status mismatch (job must still be active)", () => {
      expect(RECOVER_STALLED_LUA).toContain("current_status ~= 'active'");
      expect(RECOVER_STALLED_LUA).toContain("return 0");
    });

    it("guards against lockId mismatch (compare-and-swap)", () => {
      expect(RECOVER_STALLED_LUA).toContain("current_lock ~= expected_lock");
    });

    it("guards against lockExpiresAt mismatch", () => {
      expect(RECOVER_STALLED_LUA).toContain("current_exp ~= expected_exp");
    });

    it("recovers job atomically by re-adding to waiting set", () => {
      expect(RECOVER_STALLED_LUA).toContain("'waiting'");
      expect(RECOVER_STALLED_LUA).toContain("SREM");
      expect(RECOVER_STALLED_LUA).toContain("ZADD");
    });

    it("returns 1 on successful recovery", () => {
      expect(RECOVER_STALLED_LUA).toContain("return 1");
    });
  });

  describe("RENEW_LOCK_LUA", () => {
    it("is a non-empty string", () => {
      expect(typeof RENEW_LOCK_LUA).toBe("string");
      expect(RENEW_LOCK_LUA.trim().length).toBeGreaterThan(0);
    });

    it("guards against non-active status", () => {
      expect(RENEW_LOCK_LUA).toContain("status ~= 'active'");
    });

    it("guards against lock ownership change", () => {
      expect(RENEW_LOCK_LUA).toContain("current_lock ~= lock_id");
    });

    it("guards against already-expired locks", () => {
      expect(RENEW_LOCK_LUA).toContain("current_exp == ''");
    });

    it("updates lockExpiresAt on successful renewal", () => {
      expect(RENEW_LOCK_LUA).toContain("lockExpiresAt");
      expect(RENEW_LOCK_LUA).toContain("HSET");
    });

    it("returns 1 on successful renewal", () => {
      expect(RENEW_LOCK_LUA).toContain("return 1");
    });
  });
});
