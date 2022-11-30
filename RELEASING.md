# Release Process

1. Add release entry to [changelog](./CHANGELOG.md)
2. Open a PR with above changes.
3. Once the above PR is merged, pull the updated `main` branch down and tag the merged release commit on `main` with the new version, e.g. `git tag -a v2.3.1 -m "v2.3.1"`.
4. Push the tag, e.g. `git push origin v2.3.1`. This will kick off a CI workflow, which will publish a draft GitHub release.
5. Update Release Notes on the new draft GitHub release by generating notes with the button and review for any PR titles that could use some wordsmithing or recategorization.
