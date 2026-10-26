import { EventBus } from '../bus.js';
import { anyEventSchema } from '../events.js';
import { createLogger } from '../log.js';

const QUEUE = 'audit';

async function main(): Promise<void> {
  const logger = createLogger('audit');
  const bus = await EventBus.connect(logger);

  await bus.subscribe(QUEUE, ['image.#'], anyEventSchema, async (event) => {
    logger.info(`Recorded ${event.type}`, {
      id: event.id,
      imageId: event.data.imageId,
      correlationId: event.correlationId,
    });
  });
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
