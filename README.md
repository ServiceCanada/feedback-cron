## feedback-cj

**Page feedback cronjob:**
The purpose of this tool is to clean personal info of both the exit survey entries and page feedback entries, apply tags to page feedback entries, and sync the page feedback entries with the feedback-viewer and airtable.

## Running locally with Docker

You only need Docker and feedback-viewer checked out next to this repo; no Java, Maven, MongoDB or
secrets.

1. Clone the repository and run `docker-compose up` (or `docker compose up`) from its root.
2. The job runs once against the local MongoDB, logs what it cleaned and exits. MongoDB keeps
   running in the background.

Locally this shares one MongoDB with the feedback-viewer, like prod: both repos'
`docker-compose.yml` files use the project name `pagesuccess`. Whichever you start first creates
the database and the other reuses it, so comments the job processes show up in the local viewer
when you refresh its feedback page.

The first run downloads Maven dependencies and can take a few minutes. Locally the job never calls
Airtable or Google Sheets, and it refuses to start if the database isn't local, so nothing you do
touches production.

### Making changes

Save your Java change, then run the job again:

```sh
docker-compose run --rm job
```

It recompiles from source on every run. A compile error shows up as `[ERROR]` lines and the job
doesn't start.

### Looking at the database

MongoDB is on `localhost:27017` (database `pagesuccess`). Connect with MongoDB Compass, or open a
shell with:

```sh
docker-compose exec mongodb mongosh pagesuccess
```

The job only picks up rows it hasn't processed yet. To give it something to clean, add sample
feedback, Top Task Survey answers and profanity words with:

```sh
docker-compose exec -T mongodb mongosh --quiet --file /dev/stdin < docker/seed/seed.js
```

Each row in `docker/seed/seed.js` has a comment saying what the job should do with it: scrub
personal info, delete it as junk or a duplicate, or leave it alone. Running it again adds another
copy of the rows.

### Sharing the database with the feedback-viewer

- **Ctrl+C stops the shared MongoDB**, also for the viewer. You don't need it here: `up` returns by
  itself when the job is done. If you did press it, `docker-compose start mongodb` brings it back.
- **`docker-compose down -v` wipes the shared database**, the viewer's data included.
- **"Found orphan containers ([pagesuccess-app-1])"** is expected: the viewer's app is part of the
  same project but not of this file. Don't pass `--remove-orphans`, which removes it.

### Everyday commands

| Command                              | What it does                                     |
| ------------------------------------ | ------------------------------------------------ |
| `docker-compose up`                  | Start MongoDB and run the job once               |
| `docker-compose run --rm job`        | Run the job again (e.g. after a code change)     |
| `docker-compose logs --since 5m job` | Show the last run's output                       |
| `docker-compose stop`                | Stop MongoDB (also for the viewer); data is kept |
| `docker-compose down -v`             | Wipe the shared local database                   |
| `make test`                          | Run the unit tests                               |
| `make fmt`                           | Format all files (CI fails PRs that need it)     |

### Troubleshooting

- **`docker: 'compose' is not a docker command`:** use `docker-compose` (with a hyphen) instead.
- **Port 27017 or 8001 is already in use:** stop whatever else is using it, such as a local
  MongoDB or a feedback-viewer stack started before the two shared a project name (stop that one
  with `docker-compose -p feedback-viewer down` in the viewer's folder; its data is kept).
- **`Local mode ... only runs against MongoDB on ...`:** `SPRING_DATA_MONGODB_URI` points somewhere
  other than your machine or the compose database. Local runs refuse that on purpose.
- **Debugging:** run `DEBUG_SUSPEND=y docker-compose run --rm --service-ports job`. The job waits
  until a Java debugger attaches to `localhost:8001`.

## Domain classes from feedback-viewer

The job uses feedback-viewer's domain classes (`Problem`, `TopTaskSurvey`, their repositories,
`BadWords`, `ContentService`) from the viewer's classes-only jar
(`PageSuccess-0.0.1-SNAPSHOT-classes.jar`).

- **CI** checks out feedback-viewer at `VIEWER_REF` (set in `.github/workflows/main.yml` and
  `pr-validate.yml`) and installs it before building.
- **Docker** builds it from `../feedback-viewer` on every run, so keep feedback-viewer checked out
  next to this repo. Set `VIEWER_DIR` to use another path.
- **Without Docker**, run this once, and again whenever the viewer's domain classes change:
  ```
  mvn -f ../feedback-viewer/pom.xml -DskipTests install
  ```
