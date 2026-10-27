# How we work in this repo

## Branches
- `main` always runs. Nobody pushes to it directly.
- One branch per Trello card, named after the card: `b5-gtfs-producer`, `b7-spark-parquet`.

## Commits
- Commit small and often — at least twice a week each. The rubric checks that commits are spread across the semester.
- Message format: `B5: publish VTA positions to Kafka`.
- Commit from your own GitHub account so your work is credited.

## Pull requests
1. Push your branch and open a pull request into `main`.
2. Put the card ID in the title (`B5 – GTFS-rt producer to Kafka`) and paste the PR link into the Trello card.
3. One teammate reviews and approves, then merge.

## Pair programming
When pairing, note the driver and navigator in the PR description. Add the navigator as co-author on the last line of the commit message:
```
Co-authored-by: Name <email-on-their-github-account>
```

## Never commit
- API keys or passwords (use `.env`).
- Data files or anything over ~1 MB. Put large files on Google Drive and link them in `docs/`.
