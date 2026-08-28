import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';
import { dirname, relative, resolve } from 'node:path';
import { docRoutePolicy, markdownPathFor, splitId } from '../data/docs';
import { routeForRepoPath } from '../data/nav';
import { CURRENT, docsRoute } from '../data/versions';
import { DOCS_MODE } from '../data/site';

const markdown = (body: string, filePath: string) =>
  body.replace(/\]\(([^)#]+\.md)(#[^)]+)?\)/g, (_, target, fragment = '') => {
    if (/^[a-z]+:|^\/|^#/i.test(target)) return _;
    const path = relative('.versions/' + CURRENT.id, resolve(dirname(filePath), target)).replaceAll(
      '\\',
      '/',
    );
    return `](${routeForRepoPath(path)}${fragment})`;
  });

export async function getStaticPaths() {
  if (DOCS_MODE !== 'docs') return [];
  const docs = await getCollection('docs');
  return docs.flatMap((doc) => {
    const { version, locale, path } = splitId(doc.id);
    const policy = docRoutePolicy(docs, version, path, locale, true);
    const sibling = markdownPathFor(version, path, locale, true, policy);
    return sibling
      ? [
          {
            params: { path: `${docsRoute(version, path, locale)}/index` },
            props: { body: doc.body, filePath: doc.filePath },
          },
        ]
      : [];
  });
}

export const GET: APIRoute = ({ props }) =>
  new Response(markdown(props.body, props.filePath), {
    headers: { 'content-type': 'text/markdown; charset=utf-8' },
  });
