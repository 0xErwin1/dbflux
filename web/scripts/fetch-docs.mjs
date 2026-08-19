/**
 * Materialise each published version's documentation into `.versions/<id>/`.
 *
 * The site lives on one branch but documents several. Rather than backporting
 * it to every release branch, the build reads each version's markdown out of
 * its own git ref. `git show` is used instead of a checkout so nothing in the
 * working tree is disturbed.
 *
 * A ref that cannot be read is skipped with a warning rather than failing the
 * build: one unreachable branch should cost that version, not the whole site.
 */
import { execFileSync } from 'node:child_process';
import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

const WEB = new URL('..', import.meta.url).pathname;
const REPO = new URL('../..', import.meta.url).pathname;

export const VERSIONS_DIR = join(WEB, '.versions');

/** Paths every version contributes, matching the collection in content.config.ts. */
const WANTED =
  /^(docs\/[^/]+\.md|ARCHITECTURE\.md|CONTRIBUTING\.md|crates\/dbflux_driver_[^/]+\/README\.md)$/;

const git = (args) =>
  execFileSync('git', args, { cwd: REPO, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });

/**
 * @param {ReadonlyArray<{ id: string, ref: string }>} versions
 * @returns {string[]} ids that were materialised
 */
export function fetchDocs(versions) {
  rmSync(VERSIONS_DIR, { recursive: true, force: true });

  const done = [];

  for (const { id, ref } of versions) {
    let files;

    try {
      files = git(['ls-tree', '-r', '--name-only', ref])
        .split('\n')
        .filter((path) => WANTED.test(path));
    } catch {
      console.warn(`  docs: ref "${ref}" is unreachable — version "${id}" will be missing`);
      continue;
    }

    for (const path of files) {
      const target = join(VERSIONS_DIR, id, path);
      mkdirSync(dirname(target), { recursive: true });
      writeFileSync(target, git(['show', `${ref}:${path}`]));
    }

    console.log(`  docs: ${id} <- ${ref} (${files.length} files)`);
    done.push(id);
  }

  if (done.length === 0) {
    throw new Error('No documentation version could be read. Is this a full clone?');
  }

  return done;
}
