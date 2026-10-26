import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { EventBus } from './bus.js';
import { ROUTING_KEYS } from './config.js';
import { createEvent, type ImageUploaded } from './events.js';
import { createLogger } from './log.js';

const SCENARIOS = ['normal', 'corrupt', 'flaky', 'down', 'crash', 'duplicate', 'invalid'] as const;
type Scenario = (typeof SCENARIOS)[number];

const FILE_PREFIX: Record<Scenario, string> = {
  normal: 'photo',
  corrupt: 'corrupt',
  flaky: 'flaky',
  down: 'down',
  crash: 'crash',
  duplicate: 'photo',
  invalid: 'photo',
};

function imageUploaded(scenario: Scenario, index: number): ImageUploaded {
  const imageId = `img-${randomUUID().slice(0, 8)}`;
  return createEvent<ImageUploaded>('ImageUploaded', {
    imageId,
    fileName: `${FILE_PREFIX[scenario]}-${index}.png`,
    contentType: 'image/png',
    sizeBytes: 1_048_576 + index,
    userId: 'user-matias',
  });
}

async function main(): Promise<void> {
  const { values } = parseArgs({
    options: {
      scenario: { type: 'string', default: 'normal' },
      count: { type: 'string', default: '1' },
    },
  });
  const scenario = values.scenario as Scenario;
  if (!SCENARIOS.includes(scenario)) {
    throw new Error(`--scenario must be one of: ${SCENARIOS.join(', ')}`);
  }
  const count = Number(values.count);

  const logger = createLogger('producer');
  const bus = await EventBus.connect(logger);

  if (scenario === 'invalid') {
    await bus.publishRaw(ROUTING_KEYS.uploaded, 'this is not JSON');
    await bus.publishRaw(
      ROUTING_KEYS.uploaded,
      JSON.stringify({ type: 'ImageUploaded', data: { fileName: 'missing-fields.png' } }),
    );
  } else {
    for (let index = 1; index <= count; index++) {
      const event = imageUploaded(scenario, index);
      await bus.publish(ROUTING_KEYS.uploaded, event);
      if (scenario === 'duplicate') await bus.publish(ROUTING_KEYS.uploaded, event);
    }
  }

  await bus.close();
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
