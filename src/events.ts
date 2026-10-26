import { randomUUID } from 'node:crypto';
import { z } from 'zod';

const envelope = <K extends string, T extends z.ZodTypeAny>(type: K, data: T) =>
  z.object({
    id: z.string().uuid(),
    type: z.literal(type),
    schemaVersion: z.literal('1.0'),
    occurredAt: z.string().datetime(),
    correlationId: z.string().min(1),
    data,
  });

export const imageUploadedSchema = envelope(
  'ImageUploaded',
  z.object({
    imageId: z.string().min(1),
    fileName: z.string().min(1),
    contentType: z.enum(['image/png', 'image/jpeg', 'image/webp']),
    sizeBytes: z
      .number()
      .int()
      .positive()
      .max(25 * 1024 * 1024),
    userId: z.string().min(1),
  }),
);

export const imageProcessedSchema = envelope(
  'ImageProcessed',
  z.object({
    imageId: z.string().min(1),
    userId: z.string().min(1),
    durationMs: z.number().int().nonnegative(),
  }),
);

export const processingErrorSchema = envelope(
  'ProcessingError',
  z.object({
    imageId: z.string().min(1),
    userId: z.string().min(1),
    errorCode: z.string().min(1),
    message: z.string().min(1),
  }),
);

export const anyEventSchema = z.union([
  imageUploadedSchema,
  imageProcessedSchema,
  processingErrorSchema,
]);

export type ImageUploaded = z.infer<typeof imageUploadedSchema>;
export type ImageProcessed = z.infer<typeof imageProcessedSchema>;
export type ProcessingError = z.infer<typeof processingErrorSchema>;
export type AnyEvent = z.infer<typeof anyEventSchema>;

export function createEvent<E extends AnyEvent>(
  type: E['type'],
  data: E['data'],
  correlationId: string = randomUUID(),
): E {
  return {
    id: randomUUID(),
    type,
    schemaVersion: '1.0',
    occurredAt: new Date().toISOString(),
    correlationId,
    data,
  } as E;
}
