import type { Channel } from 'amqplib';
import { EXCHANGES, RETRY_DELAY_MS } from './config.js';

export async function declareQueue(
  channel: Channel,
  queue: string,
  bindings: string[],
): Promise<void> {
  await channel.assertExchange(EXCHANGES.events, 'topic', { durable: true });
  await channel.assertExchange(EXCHANGES.retry, 'direct', { durable: true });
  await channel.assertExchange(EXCHANGES.deadLetter, 'direct', { durable: true });

  await channel.assertQueue(queue, {
    durable: true,
    deadLetterExchange: EXCHANGES.deadLetter,
    deadLetterRoutingKey: queue,
  });
  for (const pattern of bindings) {
    await channel.bindQueue(queue, EXCHANGES.events, pattern);
  }

  const retryQueue = `${queue}.retry`;
  await channel.assertQueue(retryQueue, {
    durable: true,
    messageTtl: RETRY_DELAY_MS,
    deadLetterExchange: '',
    deadLetterRoutingKey: queue,
  });
  await channel.bindQueue(retryQueue, EXCHANGES.retry, queue);

  const deadLetterQueue = `${queue}.dlq`;
  await channel.assertQueue(deadLetterQueue, { durable: true });
  await channel.bindQueue(deadLetterQueue, EXCHANGES.deadLetter, queue);
}
