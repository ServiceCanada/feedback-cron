// Sample data for local runs:
// `docker-compose exec -T mongodb mongosh --quiet --file /dev/stdin < docker/seed/seed.js`.
// Each row says what the job should do with it; check with
// `docker-compose exec mongodb mongosh pagesuccess`.
db = db.getSiblingDB("pagesuccess");

// Word lists the cleaning step reads (BadWords in the PageSuccess jar). Upserted, so seeding
// a database that already has them doesn't fail.
[
  { word: "darn", language: "en", type: "profanity", active: true },
  { word: "zut", language: "fr", type: "profanity", active: true },
].forEach((entry) =>
  db.badwords.updateOne(
    { word: entry.word, language: entry.language, type: entry.type },
    { $set: entry },
    { upsert: true },
  ),
);

const page = {
  url: "https://www.canada.ca/en/services/benefits.html",
  title: "Benefits",
  institution: "ESDC",
  section: "Benefits",
  theme: "Benefits",
  language: "en",
  problemDate: "2026-11-02",
  timeStamp: "2026-11-02T12:00:00Z",
  deviceType: "desktop",
};

db.problem.insertMany([
  // Kept; the email, phone number and profanity are scrubbed.
  {
    ...page,
    problemDetails:
      "Darn page won't load. Email me at jane.doe@example.com or call 613-555-0199.",
  },
  // Kept; the postal code and SIN are scrubbed.
  {
    ...page,
    problemDetails: "My postal code K1A 0B1 and SIN 046 454 286 were rejected.",
  },
  // Kept as is: nothing personal in it.
  { ...page, problemDetails: "The link to the application form is broken." },
  // Deleted: duplicate of the row above.
  { ...page, problemDetails: "The link to the application form is broken." },
  // Deleted: contains HTML.
  { ...page, problemDetails: "<a href='https://spam.example'>cheap pills</a>" },
  // Deleted: submitted from the home page.
  { ...page, url: "https://www.canada.ca/", problemDetails: "Hello" },
]);

const survey = {
  dateTime: "2026-11-02 12:00",
  timeStamp: "2026-11-02T12:00:00Z",
  surveyReferrer: "https://www.canada.ca/en/services/benefits.html",
  language: "en",
  device: "desktop",
  dept: "ESDC",
  theme: "Benefits",
  task: "Apply for benefits",
  taskSatisfaction: "Satisfied",
  taskEase: "Easy",
  taskCompletion: "Yes",
  processed: "false",
};

db.toptasksurvey.insertMany([
  // Kept; the email is scrubbed from the comment.
  {
    ...survey,
    taskImproveComment: "Send updates to john.smith@example.com please",
  },
  // Kept; the whitespace-only field is emptied.
  { ...survey, taskOther: "   ", taskWhyNotComment: "Took too long" },
  // Deleted: contains HTML.
  { ...survey, taskImproveComment: "<script>alert(1)</script>" },
]);
