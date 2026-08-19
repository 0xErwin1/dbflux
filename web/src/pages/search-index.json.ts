import type { APIRoute } from 'astro';
import { getCollection, render } from 'astro:content';
import { docTitle } from '../data/nav';

interface Section {
  /** Page path. */
  p: string;
  /** Page title. */
  t: string;
  /** Section heading, empty for a page's opening text. */
  h: string;
  /** Anchor, empty when the match is the page itself. */
  a: string;
  /** Plain-text excerpt of the section. */
  s: string;
}

const SNIPPET_CHARS = 260;

/** Strip the markdown that carries no meaning once the text is a search snippet. */
function toPlainText(markdown: string): string {
  return markdown
    .replace(/```[\s\S]*?```/g, ' ')
    .replace(/`([^`]+)`/g, '$1')
    .replace(/!\[[^\]]*\]\([^)]*\)/g, ' ')
    .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
    .replace(/^\s*[>|]\s?/gm, ' ')
    .replace(/[*_~]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

/** Fallback for the rare case where rendered headings and body headings disagree. */
function slugify(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^\w\s-]/g, '')
    .trim()
    .replace(/\s+/g, '-');
}

export const GET: APIRoute = async () => {
  const docs = await getCollection('docs');
  const sections: Section[] = [];

  for (const doc of docs) {
    const { headings } = await render(doc);
    const path = `/docs/${doc.id}/`;
    const title = docTitle(doc.id);

    // Fenced code is dropped first so a `# comment` inside a shell block is
    // never mistaken for a heading.
    const body = (doc.body ?? '').replace(/```[\s\S]*?```/g, '\n');
    const lines = body.split('\n');

    const found: { depth: number; text: string; start: number }[] = [];

    lines.forEach((line, index) => {
      const match = line.match(/^(#{1,6})\s+(.+?)\s*$/);
      if (match) found.push({ depth: match[1].length, text: match[2], start: index });
    });

    const slugFor = (index: number, text: string) =>
      headings.length === found.length ? headings[index].slug : slugify(text);

    found.forEach((heading, index) => {
      const end = found[index + 1]?.start ?? lines.length;
      const text = toPlainText(lines.slice(heading.start + 1, end).join('\n'));

      if (text.length === 0 && heading.depth > 1) return;

      sections.push({
        p: path,
        t: title,
        h: heading.depth === 1 ? '' : toPlainText(heading.text),
        a: heading.depth === 1 ? '' : slugFor(index, heading.text),
        s: text.slice(0, SNIPPET_CHARS),
      });
    });
  }

  return new Response(JSON.stringify(sections), {
    headers: { 'content-type': 'application/json; charset=utf-8' },
  });
};
