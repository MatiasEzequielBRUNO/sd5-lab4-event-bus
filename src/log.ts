const COLORS = {
  producer: '\x1b[36m',
  processor: '\x1b[33m',
  notifier: '\x1b[32m',
  audit: '\x1b[35m',
  bus: '\x1b[34m',
} as const;
const RESET = '\x1b[0m';

export type Component = keyof typeof COLORS;

export function createLogger(component: Component) {
  const write = (level: string, message: string, details?: Record<string, unknown>): void => {
    const time = new Date().toISOString().slice(11, 23);
    const extra = details ? ` ${JSON.stringify(details)}` : '';
    console.log(`${time} ${COLORS[component]}[${component}]${RESET} ${level} ${message}${extra}`);
  };

  return {
    info: (message: string, details?: Record<string, unknown>) => write('INFO ', message, details),
    warn: (message: string, details?: Record<string, unknown>) => write('WARN ', message, details),
    error: (message: string, details?: Record<string, unknown>) => write('ERROR', message, details),
  };
}

export type Logger = ReturnType<typeof createLogger>;
