import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';

export default defineConfig({
  site: 'https://dbflux.dev',
  integrations: [sitemap()],
  markdown: {
    shikiConfig: {
      theme: 'ayu-dark',
      wrap: false,
    },
  },
});
