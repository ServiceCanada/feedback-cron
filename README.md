## feedback-cj

**Page feedback cronjob:**
The purpose of this tool is to clean personal info of both the exit survey entries and page feedback entries, apply tags to page feedback entries, and sync the page feedback entries with the feedback-viewer and airtable.

**Domain classes from feedback-viewer:**
The cronjob uses feedback-viewer's domain classes (`Problem`, `TopTaskSurvey`, their repositories, `BadWords`) from the viewer's classes-only jar (`PageSuccess-0.0.1-SNAPSHOT-classes.jar`).

  - **CI** checks out feedback-viewer at `VIEWER_REF` (set in `.github/workflows/main.yml` and `pr-validate.yml`) and installs it before building.
  - **Locally**, with feedback-viewer checked out next to this repo, run this once and again whenever the viewer's domain classes change:
    ```
    mvn -f ../feedback-viewer/pom.xml -DskipTests install
    ```
    Then build this repo as usual.

