import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { VERSIONS_DIR } from './fetch-docs.ts';

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
