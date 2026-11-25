import test from 'node:test';
import assert from 'node:assert/strict';
import { setupTestEnv } from './helpers/env';

setupTestEnv();

import { Keypair } from '@stellar/stellar-sdk';
import { RELEASE_LOCK_SCRIPT, RENEW_LOCK_SCRIPT } from '../src/lib/redis';
import { FakeRedis } from './helpers/fake-redis';
import {
  bindChannelLeaseToOutbox,
  channelLeaseKey,
  outboxChannelBindingKey,
  releaseChannelLeaseForOutbox,
  renewChannelLeaseForOutbox,
  setChannelLeaseRedisForTest,
} from '../src/services/stellar/channel-lease';
import { memoryStore } from './helpers/memory-outbox';

test('channel outbox lease: held through PENDING, released when row is terminal', async () => {
  const redis = new FakeRedis();
  redis.eval = async (script: string, numKeys: number, ...rest: string[]) => {
    const keys = rest.slice(0, numKeys);
    const args = rest.slice(numKeys);
    if (script === RENEW_LOCK_SCRIPT) {
      if (redis.store.get(keys[0]) !== args[0]) return 0;
      await redis.pexpire(keys[0], Number(args[1]));
      return 1;
    }
    if (script === RELEASE_LOCK_SCRIPT) {
      if (redis.store.get(keys[0]) === args[0]) {
        redis.store.delete(keys[0]);
        return 1;
      }
      return 0;
    }
    return FakeRedis.prototype.eval.call(redis, script, numKeys, ...rest);
  };
  setChannelLeaseRedisForTest(redis as never);

  const channel = Keypair.random();
  const token = '1:2:abc';
  const leaseKey = channelLeaseKey(channel.publicKey());
  await redis.set(leaseKey, token, 'PX', '60000');

  const store = memoryStore();
  const row = await store.create({ kind: 'test.write', source: channel.publicKey(), status: 'SIGNED' } as never);
  await bindChannelLeaseToOutbox(row.id, channel.publicKey(), token);

  assert.ok(redis.store.has(leaseKey), 'channel lease still held');
  assert.ok(redis.store.has(outboxChannelBindingKey(row.id)));

  await releaseChannelLeaseForOutbox(row.id, store);
  assert.ok(redis.store.has(leaseKey), 'not released while row in flight');

  await store.transition(row.id, ['SIGNED'], 'PENDING', {});
  await releaseChannelLeaseForOutbox(row.id, store);
  assert.ok(redis.store.has(leaseKey), 'still held in PENDING');

  await store.transition(row.id, ['PENDING'], 'SUCCESS', { landedHash: 'aa', returnValue: {} });
  await releaseChannelLeaseForOutbox(row.id, store);
  assert.ok(!redis.store.has(leaseKey), 'released after SUCCESS');
  assert.ok(!redis.store.has(outboxChannelBindingKey(row.id)));

  setChannelLeaseRedisForTest(null);
});

test('channel outbox lease: renew extends channel lock', async () => {
  const redis = new FakeRedis();
  let renewed = false;
  redis.eval = async (script: string, numKeys: number, ...rest: string[]) => {
    if (script === RENEW_LOCK_SCRIPT) renewed = true;
    return FakeRedis.prototype.eval.call(redis, script, numKeys, ...rest);
  };
  setChannelLeaseRedisForTest(redis as never);

  const channel = Keypair.random();
  const token = 'tok';
  await redis.set(channelLeaseKey(channel.publicKey()), token);
  const store = memoryStore();
  const row = await store.create({ kind: 'k', source: channel.publicKey() } as never);
  await bindChannelLeaseToOutbox(row.id, channel.publicKey(), token);

  await renewChannelLeaseForOutbox(row.id);
  assert.ok(renewed);

  setChannelLeaseRedisForTest(null);
});
