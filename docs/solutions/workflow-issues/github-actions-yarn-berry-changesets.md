---
title: "GitHub Actions CI/CD with Changesets for Yarn 4 Berry npm Packages"
date: 2026-05-26
category: workflow-issues
problem_type: workflow_issue
component: ci-cd-pipeline
root_cause: missing configuration patterns for Yarn Berry + Changesets integration
resolution_type: workflow_improvement
severity: medium
tags:
  - github-actions
  - changesets
  - yarn-berry
  - yarn-4
  - npm-publish
  - pnp
plan_ref: github-actions-ci-cd
---

## Problem

Setting up a complete CI/CD pipeline for npm packages using GitHub Actions, Changesets, and Yarn 4 Berry requires non-obvious configuration patterns that differ from classic Yarn/npm setups. Incorrect configuration leads to cache misses, auth failures, race conditions, and broken releases.

## Symptoms

- Release workflow publishes broken code because tests run in a separate parallel workflow
- `yarn --immutable` fails during changesets version step due to lockfile changes
- npm authentication fails when using `setup-node` registry-url with Yarn PnP
- CI cache not restored on Yarn Berry projects with `enableGlobalCache: false`
- Release workflow fails with git history errors when Changesets can't compute version bump
- Parallel release runs stomp on each other, creating conflicting version PRs
- Build scripts continue after failures, producing partial/corrupt output

## Investigation Steps

Started with a standard Changesets setup for a scoped npm package (`@dobesv/parquets`). The package uses Yarn 4 Berry with PnP linker and `enableGlobalCache: false`.

1. **Release safety gap identified**: `ci.yml` and `release.yml` trigger independently on push to `master`. No workflow dependency or branch protection ensures tests pass before publish.

2. **Yarn Berry cache behavior validated**: With `enableGlobalCache: false`, Yarn stores cache in `.yarn/cache` (local). `actions/setup-node@v4` with `cache: 'yarn'` correctly reads `.yarnrc.yml` and caches the local cache directory — no manual cache action needed.

3. **npm auth approach examined**: `setup-node` with `registry-url` writes `.npmrc` targeting `node_modules`-style installs. For Yarn PnP, manually writing `.npmrc` with `//registry.npmjs.org/:_authToken=${NODE_AUTH_TOKEN}` is more reliable.

4. **Git fetch-depth requirement discovered**: Changesets needs full git history to compute semantic version bumps from commit history. Default checkout (shallow) causes failures.

5. **Concurrency issues**: Parallel release runs on the same branch can create conflicting "Version Packages" PRs.

## Root Cause

Multiple configuration patterns differ between Yarn Classic and Yarn Berry:

1. **Release safety**: Changesets publishes on merge to master, but CI and release workflows run in parallel without dependency or branch protection gating.

2. **Yarn Berry cache location**: Yarn Berry with `enableGlobalCache: false` uses `.yarn/cache` instead of global cache. `setup-node@v4` handles this correctly by reading `.yarnrc.yml`.

3. **npm auth for PnP**: The `setup-node` registry-url approach creates `.npmrc` in a way that's optimized for `node_modules` installs. PnP's different module resolution benefits from explicit `.npmrc` creation with env var interpolation.

4. **Git history for versioning**: Changesets action needs full git history (`fetch-depth: 0`) during checkout to compute semver bumps from changeset files.

5. **Concurrency collisions**: Without concurrency group, multiple release runs on the same ref race to create/update version PRs.

6. **Script error handling**: Shell `;` separator allows subsequent commands to run after failures, producing partial builds that can be published.

## Solution

```yaml
# .github/workflows/ci.yml
name: CI

on:
  push:
    branches:
      - '**'
  pull_request:
    branches:
      - master

jobs:
  test:
    runs-on: ubuntu-latest

    strategy:
      matrix:
        node-version: [18, 20]

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Node.js ${{ matrix.node-version }}
        uses: actions/setup-node@v4
        with:
          node-version: ${{ matrix.node-version }}
          cache: 'yarn'

      - name: Install dependencies
        run: yarn --immutable

      - name: Run tests
        run: yarn test
```

```yaml
# .github/workflows/release.yml
name: Release

on:
  push:
    branches:
      - master

concurrency: ${{ github.workflow }}-${{ github.ref }}

jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 0  # Required for Changesets version computation

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: 'yarn'

      - name: Install dependencies
        run: yarn --immutable

      - name: Build and test
        run: yarn test  # Safety gate before publish

      - name: Create .npmrc
        run: |
          echo '//registry.npmjs.org/:_authToken=${NODE_AUTH_TOKEN}' > .npmrc

      - name: Create Release Pull Request or Publish
        uses: changesets/action@v1
        with:
          publish: yarn changeset publish
          version: yarn changeset version
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          NPM_TOKEN: ${{ secrets.NPM_TOKEN }}
          NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}
```

```json
// .changeset/config.json
{
  "$schema": "https://unpkg.com/@changesets/cli@2.27.0/schema.json",
  "changelog": "@changesets/cli/changelog",
  "commit": false,
  "fixed": [],
  "linked": [],
  "access": "public",
  "baseBranch": "master",
  "updateInternalDependencies": "patch",
  "ignore": []
}
```

```json
// package.json scripts - use && for fail-fast
{
  "scripts": {
    "build": "yarn clean && yarn tsc -p . && yarn tsc -p src",
    "watch": "yarn clean && yarn tsc -p . --watch",
    "test": "yarn build && yarn eslint && yarn jest --verbose test/*.ts"
  }
}
```

## Why This Works

**Release safety gate**: Running `yarn test` inside the release workflow before `changesets/action` ensures broken code cannot reach npm. Even if CI workflow fails, the release workflow itself gates on tests.

**Yarn Berry cache**: `setup-node@v4` reads `.yarnrc.yml` and correctly caches `.yarn/cache` when `enableGlobalCache: false`. No manual cache action required.

**npm auth for PnP**: Writing `.npmrc` with shell env var interpolation (`${NODE_AUTH_TOKEN}`) works reliably with Yarn PnP. The env var `NODE_AUTH_TOKEN` is set in the `changesets/action` step, and bash expands it before writing to `.npmrc`.

**fetch-depth: 0**: Changesets needs full git history to determine what changed since the last release. Shallow clone lacks the commit history and tags needed for version computation.

**Concurrency group**: `concurrency: ${{ github.workflow }}-${{ github.ref }}` ensures only one release workflow runs per branch at a time. Subsequent runs queue or cancel in-progress runs.

**Fail-fast scripts**: Using `&&` instead of `;` in package scripts ensures the chain stops on first failure. This prevents partial builds from being published.

## Prevention Strategies

**Test Cases:**
- Add integration test: push branch with intentional test failure, verify release workflow does not reach publish step
- Verify cache restore logs show "Cache restored from key" for Yarn Berry cache
- Verify changeset version step succeeds with full git history

**Best Practices:**
- Always run tests in release workflow, even if CI runs separately
- Use `fetch-depth: 0` for any workflow that needs git history (changesets, changelogs, versioning)
- Use `concurrency` group for release workflows to prevent race conditions
- Use `&&` in package scripts for fail-fast behavior
- For Yarn Berry PnP, prefer explicit `.npmrc` creation over `setup-node` `registry-url`

**Code Review Checklist:**
- [ ] Does release workflow run tests before publish?
- [ ] Is `fetch-depth: 0` set in checkout for changesets workflows?
- [ ] Is concurrency group configured for release workflow?
- [ ] Do package scripts use `&&` instead of `;`?
- [ ] Is npm auth configured correctly for Yarn Berry?

**Secrets Required:**
- `NPM_TOKEN`: npm registry authentication token (create at npmjs.com, add to repo secrets)
- `GITHUB_TOKEN`: automatically provided by GitHub Actions

## Related Issues

- **Changesets Documentation**: https://github.com/changesets/changesets/blob/main/docs/intro-to-using-changesets.md
- **Yarn Berry CI**: https://yarnpkg.com/features/caching#ci-optimization
- **actions/setup-node**: https://github.com/actions/setup-node
