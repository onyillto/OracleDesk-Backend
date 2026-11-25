/**
 * Outbox-bound channel leases (#275): once a live write signs with a leased
 * channel, the Redis lease stays held until the chain_transactions row is
 * terminal, so a CHAIN_PENDING hand-off cannot re-lease the same sequence.
 */
import { config } from '../../config';
import { logger } from '../../lib/logger';
import { getRedis, releaseLock, renewLock } from '../../lib/redis';
import { TERMINAL_STATUSES, type OutboxStore } from './outbox';

export function channelLeaseKey(publicKey: string): string {
  return `channel:lease:${publicKey}`;
}

export function outboxChannelBindingKey(outboxId: string): string {
  return `outbox:channel-lease:${outboxId}`;
}

function encodeBinding(publicKey: string, token: string): string {
  return `${publicKey}|${token}`;
}

function decodeBinding(raw: string): { publicKey: string; token: string } | null {
  const idx = raw.indexOf('|');
  if (idx <= 0) return null;
  return { publicKey: raw.slice(0, idx), token: raw.slice(idx + 1) };
}

let leaseRedisForTest: LeaseRedis | null = null;

export type LeaseRedis = Pick<ReturnType<typeof getRedis>, 'get' | 'set' | 'eval' | 'del'>;

/** Route channel-lease commands through a test double (null resets). */
export function setChannelLeaseRedisForTest(redis: LeaseRedis | null): void {
  leaseRedisForTest = redis;
}

function redisClient(): LeaseRedis {
  return (leaseRedisForTest ?? getRedis()) as LeaseRedis;
}

/** Records which channel/token guards an in-flight outbox row; the channel key stays leased. */
export async function bindChannelLeaseToOutbox(outboxId: string, publicKey: string, token: string): Promise<void> {
  const redis = redisClient();
  const ttlMs = config.CHANNEL_LEASE_MS;
  await redis.set(outboxChannelBindingKey(outboxId), encodeBinding(publicKey, token), 'PX', String(ttlMs));
}

/** Extends the Redis TTL on both the channel lock and the outbox binding. */
export async function renewChannelLeaseForOutbox(outboxId: string): Promise<void> {
  const redis = redisClient();
  const raw = await redis.get(outboxChannelBindingKey(outboxId));
  if (!raw) return;
  const binding = decodeBinding(raw);
  if (!binding) return;
  const ttlMs = config.CHANNEL_LEASE_MS;
  const ok = await renewLock(channelLeaseKey(binding.publicKey), binding.token, ttlMs, redis as never);
  if (!ok) {
    logger.warn({ outboxId, publicKey: binding.publicKey }, 'channel lease: renew failed (lost or expired)');
    return;
  }
  await redis.set(outboxChannelBindingKey(outboxId), raw, 'PX', String(ttlMs));
}

/**
 * Drops the channel lease once Postgres shows a terminal row for this outbox id.
 * No-op when the row is still in flight or the source account does not match.
 */
export async function releaseChannelLeaseForOutbox(outboxId: string, store: OutboxStore): Promise<void> {
  const redis = redisClient();
  const raw = await redis.get(outboxChannelBindingKey(outboxId));
  if (!raw) return;
  const binding = decodeBinding(raw);
  if (!binding) {
    await redis.del(outboxChannelBindingKey(outboxId)).catch(() => undefined);
    return;
  }

  const row = await store.findById(outboxId);
  if (row && !TERMINAL_STATUSES.includes(row.status as (typeof TERMINAL_STATUSES)[number])) {
    return;
  }
  if (row?.source && row.source !== binding.publicKey) {
    logger.warn(
      { outboxId, expected: binding.publicKey, source: row.source },
      'channel lease: outbox source does not match binding; refusing release',
    );
    return;
  }

  await releaseLock(channelLeaseKey(binding.publicKey), binding.token, redis as never).catch(() => undefined);
  await redis.del(outboxChannelBindingKey(outboxId)).catch(() => undefined);
  logger.debug({ outboxId, publicKey: binding.publicKey }, 'channel lease released for terminal outbox row');
}
