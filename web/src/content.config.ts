import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';

/**
 * Documentation is authored once, in the repository's `docs/` directory, and
 * read from there at build time. The site never keeps its own copy: a change to
 * driver behaviour and the paragraph describing it stay in the same commit.
 */
const docs = defineCollection({
  loader: glob({
    pattern: '*.md',
    base: '../docs',
    generateId: ({ entry }) => entry.replace(/\.md$/, '').toLowerCase(),
  }),
});

export const collections = { docs };
