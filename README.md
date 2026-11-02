## feedback-cj

**Page feedback cronjob:**
The purpose of this tool is to clean personal info of both the exit survey entries and page feedback entries, apply tags to page feedback entries, and sync the page feedback entries with the feedback-viewer and airtable.

## Running locally with Docker

You only need Docker and feedback-viewer checked out next to this repo; no Java, Maven, MongoDB or
secrets.

1. Clone the repository and run `docker-compose up` (or `docker compose up`) from its root.
2. The job runs once against a local MongoDB filled with sample data, logs what it cleaned and
   exits. Press Ctrl+C to stop MongoDB afterwards, or start with `docker-compose up -d` to keep it
   running in the background.

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

`docker/seed/seed.js` loads sample feedback (`problem`), Top Task Survey answers (`toptasksurvey`)
and profanity words (`badwords`) into a fresh database. Each row has a comment saying what the job
should do with it: scrub personal info, delete it as junk or a duplicate, or leave it alone.

MongoDB is on `localhost:27018` (database `pagesuccess`). Port 27017 is left free for the
feedback-viewer. Connect with MongoDB Compass, or open a shell with:

```sh
docker-compose exec mongodb mongosh pagesuccess
```

The job only picks up rows it hasn't processed yet. To start over with the sample data, run
`docker-compose down -v` and then `docker-compose up` again.

### Seeing the results in the feedback-viewer

To run the job against the feedback-viewer's local database (with the viewer's `docker-compose up`
running), so the cleaned rows show up in the viewer:

```sh
docker-compose run --rm --no-deps -e SPRING_DATA_MONGODB_URI=mongodb://host.docker.internal:27017 job
```

On Linux, add `--add-host=host.docker.internal:host-gateway` before `job`.

### Everyday commands

| Command                                | What it does                                       |
| -------------------------------------- | -------------------------------------------------- |
| `docker-compose up`                    | Start MongoDB and run the job once                 |
| `docker-compose run --rm job`          | Run the job again (e.g. after a code change)       |
| `docker-compose logs --since 5m job`   | Show the last run's output                         |
| `docker-compose stop`                  | Stop MongoDB; your data is kept                    |
| `docker-compose down -v`               | Wipe the local database (re-seeded on next start)  |
| `make test`                            | Run the unit tests                                 |

### Troubleshooting

- **`docker: 'compose' is not a docker command`:** use `docker-compose` (with a hyphen) instead.
- **Port 27018 or 8001 is already in use:** stop whatever else is using it.
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
