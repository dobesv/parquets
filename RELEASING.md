# Releasing

This project uses [Changesets](https://github.com/changesets/changesets) for
versioning and [GitHub Actions](/.github/workflows/release.yml) for automated
publishing to npm.

---

## Prerequisites (one-time setup)

- `NPM_TOKEN` secret must be set in **GitHub → Settings → Secrets and variables
  → Actions** with publish rights to `@dobesv/parquets`.
- The `github-actions` branch (or whichever branch contains the CI/CD setup)
  must be merged into `master`.

---

## Normal workflow: releasing changes as you go

### 1. Add a changeset alongside your code changes

Every PR that should appear in the changelog needs a changeset file.

```bash
yarn changeset
```

The interactive prompt asks:
- **Bump type** — choose one:
  - `patch` — bug fixes, internal changes (0.11.9 → 0.11.10)
  - `minor` — new features, backwards-compatible (0.11.9 → 0.12.0)
  - `major` — breaking changes (0.11.9 → 1.0.0)
- **Summary** — one line describing the change (goes into `CHANGELOG.md`)

This creates a file like `.changeset/fuzzy-lions-eat.md`. Commit it with your
code changes and include it in your PR.

### 2. Merge your PR to `master`

The [Release workflow](/.github/workflows/release.yml) runs automatically on
every push to `master`. If there are pending changesets it opens (or updates)
a PR titled **"Version Packages"**.

### 3. Review and merge the "Version Packages" PR

The "Version Packages" PR:
- Bumps the version in `package.json` according to the accumulated changesets
- Updates `CHANGELOG.md` with all the changeset summaries
- Deletes the consumed `.changeset/*.md` files

Review it, then **merge it into `master`**.

### 4. Automatic publish

Merging the "Version Packages" PR triggers the release workflow again. This
time there are no pending changesets, so instead of opening a PR it:
1. Builds the package (`yarn test` which includes `yarn build`)
2. Publishes to npm (`yarn changeset publish`)
3. Creates a git tag (e.g. `@dobesv/parquets@0.12.0`)

The new version appears on npm within a minute or two.

---

## Checking release status

```bash
# See what changesets are pending (not yet versioned)
yarn changeset status

# See what would be published (dry run)
yarn changeset publish --dry-run
```

---

## Manual release (if automation fails)

If the GitHub Actions release workflow fails or you need to publish from your
local machine:

```bash
# 1. Make sure you're on master and up to date
git checkout master && git pull

# 2. Authenticate with npm
npm login  # or set NPM_TOKEN in your environment

# 3. Apply pending changesets (bumps version, updates CHANGELOG)
yarn changeset version

# 4. Commit the version bump
git add -A && git commit -m "chore: version packages"

# 5. Build and publish
yarn changeset publish

# 6. Push the tag and the commit
git push --follow-tags
```

---

## Semver reference

| Change type | Example | When to use |
|-------------|---------|-------------|
| `patch` | `0.11.9` → `0.11.10` | Bug fixes, dependency updates, docs |
| `minor` | `0.11.9` → `0.12.0` | New features, new options (backwards-compatible) |
| `major` | `0.11.9` → `1.0.0` | Breaking API changes |

---

## Troubleshooting

**"Version Packages" PR not appearing after merging to master**
- Check the [Actions tab](https://github.com/dobesv/parquets/actions) for the
  Release workflow run. Look for errors in the `changesets/action` step.
- Make sure at least one `.changeset/*.md` file was committed (run
  `yarn changeset status` to check).

**Publish step fails with 403 / authentication error**
- Verify the `NPM_TOKEN` secret is set and the token has `Automation` type with
  publish access to the `@dobesv` scope.

**Build fails before publish**
- The release workflow runs `yarn test` before publishing. Fix the failing
  tests/build, push to master, and the "Version Packages" PR will republish on
  the next merge.
