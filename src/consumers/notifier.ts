import { z } from 'zod';
import { EventBus } from '../bus.js';
import { ROUTING_KEYS } from '../config.js';
import { imageProcessedSchema, processingErrorSchema } from '../events.js';
import { createLogger } from '../log.js';

const QUEUE = 'notifications';

async function main(): Promise<void> {
  const logger = createLogger('notifier');
  const bus = await EventBus.connect(logger);
  const schema = z.union([imageProcessedSchema, processingErrorSchema]);

  await bus.subscribe(
    QUEUE,
    [ROUTING_KEYS.processed, ROUTING_KEYS.error],
    schema,
    async (event) => {
      if (event.type === 'ImageProcessed') {
        logger.info(`Notify ${event.data.userId}: image ${event.data.imageId} is ready`, {
          durationMs: event.data.durationMs,
        });
      } else {
        logger.warn(`Notify ${event.data.userId}: image ${event.data.imageId} failed`, {
          errorCode: event.data.errorCode,
        });
      }
    },
  );
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
