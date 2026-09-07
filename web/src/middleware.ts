import { defineMiddleware } from 'astro/middleware';

/**
 * `i18n.routing: 'manual'` disables Astro's built-in i18n middleware and
 * requires the project to provide its own — even for a fully static build,
 * where locale is already baked into each generated path and there is no
 * per-request detection to do. This middleware exists to satisfy that
 * requirement; it does no locale work itself.
 */
export const onRequest = defineMiddleware((_context, next) => next());
