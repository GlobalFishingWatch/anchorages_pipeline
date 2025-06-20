[Git Flow]: https://nvie.com/posts/a-successful-git-branching-model/
[Semantic Versioning]: https://semver.org

> [!IMPORTANT]
In the following, **MAJOR**, **MINOR** and **PATCH** refer to [Semantic Versioning].

We use [Git Flow] as our branching strategy, which is well suited for projects with long release cycles.
We add also support for maintanance of previous **MAJOR** versions.
Here we present a summary of the strategy.

## **Main branches**:

* **master**: reflects the code in production for the current **MAJOR**.
* **develop**: integrates code for the next release.
* **support/x.y**: reflects code in production for previous **MAJOR**,
allowing for **PATCH** increments. 

## **Feature branches**:

1. Create a branch from **develop**.
2. Work on the feature.
3. Rebase on-top of **develop**.
3. Merge back to **develop** with a merge commit.

There is currently no convention about the name of the feature branches,
other than being descriptive and separate the words using hyphens "-".

To maintain a clear semi-linear history in **develop**,
we rebease on top of **develop** before merging the branch.
The merge should be done **forcing a merge commit**,
otherwise would be a fast-forward merge (because we rebased)
and the history would be linear instead of semi-linear,
losing the context of the branch.
This is enforced in the GitHub UI,
but locally is done with:
```shell
git merge branch --no-ff
```

## **Release branches**:

1. Create a branch named **release/x.y.z** from **develop**.
2. Perform all steps needed to make the release.
3. Merge to **master** and **develop**.
4. Tag **master**.

If the release its a **MAJOR** release, create a branch **support/x.y**,
where "x" is the previous **MAJOR** and "y" its last **MINOR** release.

## **Hotfix branches**:

These are quick **PATCH** increments done only to current **MAJOR** release:

1. Create a branch named **hotfix/x.y.z** from **master**.
2. Work on the fix. Perform steps needed to make the release.
3. Merge back to **master** and also to **develop**.
4. Tag **master**.

## **Patch branches**:

These are **PATCH** increments to previous **MAJOR** releases:

1. Create a branch named **patch/my-patch** from **support/x.y**.
2. Work on the fix. Perform steps needed to make a release.
3. Merge to **support/x.y**.
4. Tag **support/x.y**. 

If you need also this fix in the current **MAJOR**,
you will have to re-work the fix to be compatible with the new API.
You can work it in a [feature branch](#feature-branches) or make a [hotfix](#hotfix-branches).