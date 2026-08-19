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
import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
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
 * List a ref's tracked paths, fetching the ref first if this clone lacks it.
 *
 * Build platforms clone shallowly and usually check out one branch, so the other
 * release branches are simply absent. A targeted fetch is cheaper than asking
 * every deployment to clone the full history, and it fails loudly enough to be
 * caught by the caller when there is no network or no remote.
 */
function readTree(ref) {
  const list = () =>
    git(['ls-tree', '-r', '--name-only', ref])
      .split('\n')
      .filter((path) => WANTED.test(path));

  try {
    return list();
  } catch {
    git(['fetch', '--depth=1', '--no-tags', 'origin', `${ref}:${ref}`]);
    return list();
  }
}

/**
 * The product version a ref documents.
 *
 * `docs/RELEASE.md` names the workspace version in Cargo.toml as the source of
 * truth and requires every other manifest to stay in lockstep, so it is the one
 * place worth reading. Hard-coding these in the site is how the 0.6 entry ended
 * up claiming a release that was never cut.
 */
function workspaceVersion(ref) {
  const manifest = git(['show', `${ref}:Cargo.toml`]);
  const match = manifest.match(/^version\s*=\s*"([^"]+)"/m);

  if (!match) throw new Error(`No workspace version in ${ref}:Cargo.toml`);

  return match[1];
}

/**
 * What the documentation was built from.
 *
 * The version number alone does not identify a moving target: nightly reports
 * the same `-dev` number for months, and a release branch keeps its number
 * across cherry-picked fixes.
 */
function buildOf(ref) {
  const [commit, date] = git(['show', '-s', '--format=%h%n%cs', ref]).trim().split('\n');

  return { commit, date };
}

/**
 * @param {ReadonlyArray<{ id: string, ref: string }>} versions
 * @returns {Array<{ id: string, ref: string, version: string }>} what was materialised
 */
export function fetchDocs(versions) {
  rmSync(VERSIONS_DIR, { recursive: true, force: true });

  const done = [];

  for (const { id, ref } of versions) {
    let files;

    try {
      files = readTree(ref);
    } catch {
      console.warn(`  docs: ref "${ref}" is unreachable — version "${id}" will be missing`);
      continue;
    }

    for (const path of files) {
      const target = join(VERSIONS_DIR, id, path);
      mkdirSync(dirname(target), { recursive: true });
      writeFileSync(target, git(['show', `${ref}:${path}`]));
    }

    const version = workspaceVersion(ref);
    const { commit, date } = buildOf(ref);

    console.log(`  docs: ${id} <- ${ref} @ ${version} (${commit}, ${files.length} files)`);
    done.push({ id, ref, version, commit, date });
  }

  writeFileSync(join(VERSIONS_DIR, 'manifest.json'), JSON.stringify(done, null, 2));

  if (done.length === 0) {
    throw new Error('No documentation version could be read. Is this a full clone?');
  }

  return done;
}

const registry = JSON.parse(readFileSync(join(WEB, 'versions.json'), 'utf8'));

fetchDocs(registry);
