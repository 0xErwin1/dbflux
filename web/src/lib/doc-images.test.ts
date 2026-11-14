import assert from 'node:assert/strict';
import test from 'node:test';
import { isDocImagePath, rewriteDocImages, rewriteMarkdownImages } from './doc-images.ts';

const urlFor = (repoPath: string, versionId: string) => `/served/${versionId}/${repoPath}`;

function rewrite(filePath: string, value: string): string {
  const tree = { type: 'root', children: [{ type: 'raw', value }] };
  rewriteDocImages(tree, filePath, urlFor);
  return tree.children[0].value;
}

const english = '/repo/web/.versions/nightly/docs/USAGE.md';
const spanish = '/repo/web/.versions/v0.7/docs/es/USAGE.md';

test('accepts only image files under docs/images', () => {
  assert.equal(isDocImagePath('docs/images/usage/main-light.webp'), true);
  assert.equal(isDocImagePath('docs/images/usage/main-dark.png'), true);
  assert.equal(isDocImagePath('docs/images/settings/panel.jpg'), true);
  assert.equal(isDocImagePath('docs/images/settings/panel.jpeg'), true);
  assert.equal(isDocImagePath('docs/images/diagram.svg'), true);

  assert.equal(isDocImagePath('docs/images/usage/clip.gif'), false);
  assert.equal(isDocImagePath('docs/images/usage/notes.md'), false);
  assert.equal(isDocImagePath('docs/es/images/usage/main-light.webp'), false);
  assert.equal(isDocImagePath('resources/dbflux.png'), false);
});

test('rewrites the picture form to the served copy of the page version', () => {
  const html = [
    '<picture>',
    '  <source media="(prefers-color-scheme: dark)" srcset="images/usage/main-window-dark.webp">',
    '  <img src="images/usage/main-window-light.webp" alt="The main window">',
    '</picture>',
  ].join('\n');

  assert.equal(
    rewrite(english, html),
    [
      '<picture>',
      '  <source media="(prefers-color-scheme: dark)" srcset="/served/nightly/docs/images/usage/main-window-dark.webp">',
      '  <img src="/served/nightly/docs/images/usage/main-window-light.webp" alt="The main window">',
      '</picture>',
    ].join('\n'),
  );
});

test('resolves a translation path against its own directory', () => {
  assert.equal(
    rewrite(spanish, `<img src='../images/usage/a.webp' alt="x">`),
    `<img src='/served/v0.7/docs/images/usage/a.webp' alt="x">`,
  );
  assert.equal(
    rewrite(spanish, '<source srcset="../images/usage/a-dark.webp">'),
    '<source srcset="/served/v0.7/docs/images/usage/a-dark.webp">',
  );
});

test('keeps a query or fragment on the rewritten path', () => {
  assert.equal(
    rewrite(english, '<img src="images/a.svg#icon">'),
    '<img src="/served/nightly/docs/images/a.svg#icon">',
  );
});

test('leaves absolute, protocol-relative, root-relative and data sources untouched', () => {
  for (const html of [
    '<img src="https://hosted.weblate.org/widget/dbflux/multi-auto.svg" alt="x">',
    '<img src="//cdn.example.com/a.webp">',
    '<img src="/brand/mark.svg">',
    '<img src="data:image/png;base64,AAAA">',
    '<source srcset="https://example.com/a.webp">',
  ]) {
    assert.equal(rewrite(english, html), html);
  }
});

test('leaves a source outside docs/images untouched so the link check reports it', () => {
  for (const html of [
    '<img src="../resources/dbflux.png">',
    '<img src="images/usage/clip.gif">',
    '<img src="../../../../outside.webp">',
  ]) {
    assert.equal(rewrite(english, html), html);
  }
});

test('leaves a multi-candidate srcset untouched', () => {
  const html = '<source srcset="images/a.webp 1x, images/a@2x.webp 2x">';
  assert.equal(rewrite(english, html), html);
});

test('does not touch other tags or attributes that merely look alike', () => {
  const html = '<a href="images/a.webp" data-src="images/a.webp">images/a.webp</a>';
  assert.equal(rewrite(english, html), html);
});

test('rewrites markdown images and takes them away from the Astro image pipeline', () => {
  const rewritten = {
    type: 'element',
    tagName: 'img',
    properties: { src: 'images/usage/a.webp', alt: 'x' },
    children: [],
  };
  const kept = {
    type: 'element',
    tagName: 'img',
    properties: { src: '../resources/dbflux.png', alt: 'y' },
    children: [],
  };
  const tree = { type: 'root', children: [rewritten, kept] };
  const data = { astro: { localImagePaths: ['images/usage/a.webp', '../resources/dbflux.png'] } };

  rewriteDocImages(tree, english, urlFor, data);

  assert.equal(rewritten.properties.src, '/served/nightly/docs/images/usage/a.webp');
  assert.equal(kept.properties.src, '../resources/dbflux.png');
  assert.deepEqual(data.astro.localImagePaths, ['../resources/dbflux.png']);
});

test('ignores files that are not inside a version mirror', () => {
  const html = '<img src="images/usage/a.webp">';
  assert.equal(rewrite('/repo/web/src/pages/index.md', html), html);
});

test('reaches raw nodes nested inside elements', () => {
  const raw = { type: 'raw', value: '<img src="images/a.webp">' };
  const tree = {
    type: 'root',
    children: [{ type: 'element', tagName: 'p', properties: {}, children: [raw] }],
  };

  rewriteDocImages(tree, english, urlFor);

  assert.equal(raw.value, '<img src="/served/nightly/docs/images/a.webp">');
});

test('rewrites every image form in a markdown copy of a page', () => {
  const body = [
    '# Usage',
    '',
    '<picture>',
    '  <source media="(prefers-color-scheme: dark)" srcset="images/usage/main-dark.webp">',
    '  <img src="images/usage/main-light.webp" alt="The main window">',
    '</picture>',
    '',
    '![Plain](images/usage/plain.webp) and ![Titled](images/usage/titled.png "A title")',
    '',
    '[Settings](SETTINGS.md) ![Remote](https://example.com/a.webp) ![Other](../resources/dbflux.png)',
  ].join('\n');

  assert.equal(
    rewriteMarkdownImages(body, '.versions/nightly/docs/USAGE.md', urlFor),
    [
      '# Usage',
      '',
      '<picture>',
      '  <source media="(prefers-color-scheme: dark)" srcset="/served/nightly/docs/images/usage/main-dark.webp">',
      '  <img src="/served/nightly/docs/images/usage/main-light.webp" alt="The main window">',
      '</picture>',
      '',
      '![Plain](/served/nightly/docs/images/usage/plain.webp) and ![Titled](/served/nightly/docs/images/usage/titled.png "A title")',
      '',
      '[Settings](SETTINGS.md) ![Remote](https://example.com/a.webp) ![Other](../resources/dbflux.png)',
    ].join('\n'),
  );
});

test('resolves a translation markdown copy against its own directory', () => {
  assert.equal(
    rewriteMarkdownImages(
      '![Ventana](../images/usage/a.webp)\n<img src="../images/usage/b.webp">',
      '/repo/web/.versions/v0.7/docs/es/USAGE.md',
      urlFor,
    ),
    '![Ventana](/served/v0.7/docs/images/usage/a.webp)\n<img src="/served/v0.7/docs/images/usage/b.webp">',
  );
});

test('leaves code in a markdown copy exactly as written', () => {
  const body = [
    'Use `![alt](images/a.webp)` inline.',
    '',
    '```html',
    '<img src="images/a.webp">',
    '![alt](images/a.webp)',
    '```',
    '',
    '~~~',
    '<source srcset="images/a.webp">',
    '~~~',
  ].join('\n');

  assert.equal(rewriteMarkdownImages(body, english, urlFor), body);
});
