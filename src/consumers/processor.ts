import { setTimeout as sleep } from 'node:timers/promises';
import { EventBus, TransientError } from '../bus.js';
import { ROUTING_KEYS } from '../config.js';
import {
  createEvent,
  imageUploadedSchema,
  type ImageProcessed,
  type ProcessingError,
} from '../events.js';
import { createLogger } from '../log.js';

const QUEUE = 'image.processing';

async function main(): Promise<void> {
  const logger = createLogger('processor');
  const instance = process.env.INSTANCE ?? `pid-${process.pid}`;
  const bus = await EventBus.connect(logger);

  await bus.subscribe(
    QUEUE,
    [ROUTING_KEYS.uploaded],
    imageUploadedSchema,
    async (event, context) => {
      const { imageId, fileName, userId } = event.data;
      logger.info(`Processing ${fileName}`, {
        instance,
        imageId,
        retry: context.retryCount,
        redelivered: context.redelivered,
      });

      if (fileName.startsWith('crash-') && !context.redelivered) {
        logger.error('Simulated crash before acknowledging the message', { instance, imageId });
        process.exit(1);
      }
      if (fileName.startsWith('down-')) {
        throw new TransientError('storage unavailable');
      }
      if (fileName.startsWith('flaky-') && context.retryCount < 2) {
        throw new TransientError('storage timeout');
      }
      if (fileName.startsWith('corrupt-')) {
        await bus.publish(
          ROUTING_KEYS.error,
          createEvent<ProcessingError>(
            'ProcessingError',
            {
              imageId,
              userId,
              errorCode: 'corrupt-image',
              message: 'The file is not a valid image',
            },
            event.correlationId,
          ),
        );
        return;
      }

      const started = Date.now();
      await sleep(500 + Math.floor(Math.random() * 1000));
      await bus.publish(
        ROUTING_KEYS.processed,
        createEvent<ImageProcessed>(
          'ImageProcessed',
          { imageId, userId, durationMs: Date.now() - started },
          event.correlationId,
        ),
      );
    },
  );
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
