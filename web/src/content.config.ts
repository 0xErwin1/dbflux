import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';

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
 */
const docs = defineCollection({
  loader: glob({
    base: '.versions',
    pattern: [
      '*/docs/*.md',
      '*/ARCHITECTURE.md',
      '*/CONTRIBUTING.md',
      '*/crates/dbflux_driver_*/README.md',
    ],
    generateId: ({ entry }) => {
      const [version, ...rest] = entry.split('/');
      const path = rest.join('/');

      const driver = path.match(/^crates\/dbflux_driver_([^/]+)\/README\.md$/);
      if (driver) return `${version}/drivers/${driver[1]}`;

      return `${version}/${path
        .replace(/^docs\//, '')
        .replace(/\.md$/, '')
        .toLowerCase()}`;
    },
  }),
});

export const collections = { docs };
