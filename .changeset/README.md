# Changesets

This project uses [Changesets](https://github.com/changesets/changesets) to manage versions and changelogs.

## How to Add a Changeset

When you make a change that should be included in the next release:

1. Run `yarn changeset` (or `yarn changeset add`).
2. Select the change type:
   - **major**: Breaking changes (1.0.0 → 2.0.0)
   - **minor**: New features, no breaking changes (1.0.0 → 1.1.0)
   - **patch**: Bug fixes, no breaking changes (1.0.0 → 1.0.1)
3. Write a brief description of the change.
4. Commit the generated `.changeset/*.md` file along with your code.

## Release Process

The release process is automated via GitHub Actions:

1. When PRs are merged to `master`, the [Release workflow](/.github/workflows/release.yml) runs.
2. If there are pending changesets, it opens or updates a "Version Packages" PR.
3. When the "Version Packages" PR is merged:
   - Packages are versioned according to the changesets.
   - Packages are published to npm.
   - Git tags are created for each published version.

## Manual Release (if needed)

If you need to release manually:

```bash
# Version bump based on changesets
yarn changeset version

# Publish to npm
yarn changeset publish
```
