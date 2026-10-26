import amqp, { type ConfirmChannel, type ConsumeMessage } from 'amqplib';
import type { z } from 'zod';
import { AMQP_URL, EXCHANGES, MAX_RETRIES } from './config.js';
import type { AnyEvent } from './events.js';
import type { Logger } from './log.js';
import { declareQueue } from './topology.js';

export class TransientError extends Error {}

export interface MessageContext {
  retryCount: number;
  redelivered: boolean;
}

type Handler<E> = (event: E, context: MessageContext) => Promise<void>;

export class EventBus {
  private readonly processedIds = new Set<string>();

  private constructor(
    private readonly connection: Awaited<ReturnType<typeof amqp.connect>>,
    private readonly channel: ConfirmChannel,
    private readonly logger: Logger,
  ) {}

  static async connect(logger: Logger): Promise<EventBus> {
    const connection = await amqp.connect(AMQP_URL);
    const channel = await connection.createConfirmChannel();
    await channel.assertExchange(EXCHANGES.events, 'topic', { durable: true });
    logger.info('Connected to RabbitMQ', { exchange: EXCHANGES.events });
    return new EventBus(connection, channel, logger);
  }

  async publish(routingKey: string, event: AnyEvent): Promise<void> {
    this.channel.publish(EXCHANGES.events, routingKey, Buffer.from(JSON.stringify(event)), {
      persistent: true,
      contentType: 'application/json',
      messageId: event.id,
      type: event.type,
      correlationId: event.correlationId,
    });
    await this.channel.waitForConfirms();
    this.logger.info(`Published ${event.type}`, { routingKey, id: event.id });
  }

  async publishRaw(routingKey: string, content: string): Promise<void> {
    this.channel.publish(EXCHANGES.events, routingKey, Buffer.from(content), { persistent: true });
    await this.channel.waitForConfirms();
    this.logger.warn('Published a raw message', { routingKey, content });
  }

  async subscribe<S extends z.ZodTypeAny>(
    queue: string,
    bindings: string[],
    schema: S,
    handler: Handler<z.infer<S>>,
  ): Promise<void> {
    await declareQueue(this.channel, queue, bindings);
    await this.channel.prefetch(1);
    await this.channel.consume(queue, (message) => {
      if (message) void this.handle(queue, message, schema, handler);
    });
    this.logger.info(`Waiting for events on queue "${queue}"`, { bindings });
  }

  private async handle<S extends z.ZodTypeAny>(
    queue: string,
    message: ConsumeMessage,
    schema: S,
    handler: Handler<z.infer<S>>,
  ): Promise<void> {
    let payload: unknown;
    try {
      payload = JSON.parse(message.content.toString());
    } catch {
      this.deadLetter(message, 'invalid JSON');
      return;
    }

    const parsed = schema.safeParse(payload);
    if (!parsed.success) {
      const issues = parsed.error.issues.map(
        (issue: z.ZodIssue) => `${issue.path.join('.')}: ${issue.message}`,
      );
      this.deadLetter(message, `schema validation failed (${issues.join('; ')})`);
      return;
    }

    const event = parsed.data as AnyEvent;
    if (this.processedIds.has(event.id)) {
      this.logger.warn('Duplicate event skipped', { id: event.id, type: event.type });
      this.channel.ack(message);
      return;
    }

    const retryCount = Number(message.properties.headers?.['x-retry-count'] ?? 0);
    try {
      await handler(parsed.data, { retryCount, redelivered: message.fields.redelivered });
      this.processedIds.add(event.id);
      this.channel.ack(message);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      if (error instanceof TransientError && retryCount < MAX_RETRIES) {
        this.channel.publish(EXCHANGES.retry, queue, message.content, {
          ...message.properties,
          headers: { ...message.properties.headers, 'x-retry-count': retryCount + 1 },
        });
        await this.channel.waitForConfirms();
        this.channel.ack(message);
        this.logger.warn(`Retry ${retryCount + 1}/${MAX_RETRIES} scheduled`, {
          id: event.id,
          reason,
        });
        return;
      }
      this.deadLetter(
        message,
        error instanceof TransientError ? `${reason} after ${MAX_RETRIES} retries` : reason,
      );
    }
  }

  private deadLetter(message: ConsumeMessage, reason: string): void {
    this.channel.nack(message, false, false);
    this.logger.error('Message sent to the dead-letter queue', { reason });
  }

  async close(): Promise<void> {
    await this.channel.close();
    await this.connection.close();
  }
}
