import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';

/**
 * Documentation is authored once, in the repository, and read from there at
 * build time. The site never keeps its own copy: a change to driver behaviour
 * and the paragraph describing it stay in the same commit.
 *
 * Everything a reader might be sent to lives here, including the driver READMEs
 * and the architecture and contributing guides. The site does not hand people
 * off to the repository to finish reading.
 */
const docs = defineCollection({
  loader: glob({
    base: '..',
    pattern: ['docs/*.md', 'ARCHITECTURE.md', 'CONTRIBUTING.md', 'crates/dbflux_driver_*/README.md'],
    generateId: ({ entry }) => {
      const driver = entry.match(/^crates\/dbflux_driver_([^/]+)\/README\.md$/);
      if (driver) return `drivers/${driver[1]}`;

      return entry
        .replace(/^docs\//, '')
        .replace(/\.md$/, '')
        .toLowerCase();
    },
  }),
});

export const collections = { docs };
