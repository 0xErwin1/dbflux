import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';
import { contentEntryId, docsRepoPatterns } from './i18n/locale-registry.ts';

/**
 * Documentation for every published version.
 *
 * Files are materialised into `.versions/<version>/` from each version's git ref
 * before the collection loads, so the site can document several releases while
 * living on a single branch. Entry ids are `<version>/<page>` — the current
 * release is served unprefixed and the rest keep their version in the URL.
 *
 * The site never keeps its own copy of a document: the driver READMEs and the
 * architecture and contributing guides are read from the repository too, so a
 * change in behaviour and the paragraph describing it stay in one commit.
 *
 * Localized siblings live in the explicit docs directories from the locale
 * registry. Their generated ids use the canonical locale id, even when its
 * filesystem directory has different casing or spelling.
 */
const docs = defineCollection({
  loader: glob({
    base: '.versions',
    pattern: [
      ...docsRepoPatterns().map((pattern) => `*/${pattern}`),
      '*/ARCHITECTURE.md',
      '*/CONTRIBUTING.md',
      '*/SECURITY.md',
      '*/crates/dbflux_driver_*/README.md',
    ],
    generateId: ({ entry }) => contentEntryId(entry),
  }),
});

export const collections = { docs };
