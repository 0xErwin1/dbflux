import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { fetchDocs, VERSIONS_DIR } from './fetch-docs.ts';

const REPO = fileURLToPath(new URL('../../', import.meta.url));
const ref = 'main';
const asset = 'resources/dbflux.png';

test('materialized docs mirror image bytes without adding an asset source fact', () => {
  // Importing the materializer runs its configured registry; .versions is ignored build output.
  const manifest = JSON.parse(readFileSync(join(VERSIONS_DIR, 'manifest.json'), 'utf8')) as Array<{
    id: string;
    ref: string;
    sourceFacts: Array<{ path: string }>;
  }>;
  const version = manifest.find((entry) => entry.ref === ref);
  assert.ok(version, `configured local ref ${ref} must be materialized`);

  const expected = execFileSync('git', ['show', `${ref}:${asset}`], { cwd: REPO });
  const mirrored = join(VERSIONS_DIR, version.id, asset);
  assert.ok(existsSync(mirrored), `missing mirrored image: ${mirrored}`);
  assert.deepEqual(readFileSync(mirrored), expected);
  assert.equal(
    version.sourceFacts.some((fact) => fact.path === asset),
    false,
  );
});

/**
 * Write `files` into a commit object that no branch points at, so a test can
 * materialise a ref with exactly the paths it needs without touching the
 * working tree, the index or any ref. The objects are unreferenced and left to
 * `git gc`.
 */
function fixtureCommit(files: Record<string, Buffer>, scratch: string): string {
  const env = {
    ...process.env,
    GIT_INDEX_FILE: join(scratch, 'index'),
    GIT_AUTHOR_NAME: 'fixture',
    GIT_AUTHOR_EMAIL: 'fixture@example.invalid',
    GIT_COMMITTER_NAME: 'fixture',
    GIT_COMMITTER_EMAIL: 'fixture@example.invalid',
  };
  const git = (args: string[], input?: Buffer) =>
    execFileSync('git', args, { cwd: REPO, env, input }).toString().trim();

  for (const [path, bytes] of Object.entries(files)) {
    const blob = git(['hash-object', '-w', '--stdin'], bytes);
    git(['update-index', '--add', '--cacheinfo', `100644,${blob},${path}`]);
  }

  return git(['commit-tree', git(['write-tree']), '-m', 'fixture']);
}

test('materialized versions copy docs images byte for byte and nothing else under docs/images', () => {
  const scratch = mkdtempSync(join(tmpdir(), 'fetch-docs-'));

  try {
    // Bytes that are not valid UTF-8, so a text round trip would corrupt them.
    const webp = Buffer.from([0x52, 0x49, 0x46, 0x46, 0xff, 0xfe, 0x00, 0x80, 0x57, 0x45]);
    const png = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0xc3, 0x28]);
    const jpg = Buffer.from([0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0xa0, 0xa1]);
    const svg = Buffer.from('<svg xmlns="http://www.w3.org/2000/svg"/>');

    const commit = fixtureCommit(
      {
        'Cargo.toml': Buffer.from('[workspace.package]\nversion = "9.9.9"\n'),
        'docs/USAGE.md': Buffer.from('# Usage\n'),
        'docs/images/usage/main-light.webp': webp,
        'docs/images/usage/main-dark.png': png,
        'docs/images/settings/panel.jpg': jpg,
        'docs/images/diagram.svg': svg,
        'docs/images/usage/clip.gif': Buffer.from('GIF89a'),
        'docs/images/usage/notes.txt': Buffer.from('not an image'),
      },
      scratch,
    );

    const output = join(scratch, 'versions');
    const [version] = fetchDocs([{ id: 'fixture', ref: commit }], output);
    const root = join(output, 'fixture');

    assert.deepEqual(readFileSync(join(root, 'docs/images/usage/main-light.webp')), webp);
    assert.deepEqual(readFileSync(join(root, 'docs/images/usage/main-dark.png')), png);
    assert.deepEqual(readFileSync(join(root, 'docs/images/settings/panel.jpg')), jpg);
    assert.deepEqual(readFileSync(join(root, 'docs/images/diagram.svg')), svg);
    assert.equal(existsSync(join(root, 'docs/images/usage/clip.gif')), false);
    assert.equal(existsSync(join(root, 'docs/images/usage/notes.txt')), false);

    assert.deepEqual(
      version.sourceFacts.map((fact) => fact.path),
      ['usage'],
    );
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
});
