export const AMQP_URL = process.env.AMQP_URL ?? 'amqp://lab:lab@localhost:5672';

export const EXCHANGES = {
  events: 'images.events',
  retry: 'images.retry',
  deadLetter: 'images.dlx',
} as const;

export const ROUTING_KEYS = {
  uploaded: 'image.uploaded',
  processed: 'image.processed',
  error: 'image.error',
} as const;

export const MAX_RETRIES = 3;
export const RETRY_DELAY_MS = 3_000;
