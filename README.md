# Quartz Pebble

This is a small bot to help our team with tracking bench time. It uses a list of Warcraft Logs reports to figure out who
is present when we're fighting mythic bosses, computes time spent on the bench vs. being absent, and lets officers make
tweaks to the results as necessary.

ChatGPT and Claude generated 98%+ of the code under my careful guidance. I've reviewed all of it and tested it
extensively since TWW S2.

## What can I see?

The web app shows published bench-time results and attendance without requiring
an account. You can browse raid nights, look at a player's time breakdown, and
share a link to a result.

Officers sign in with Google to manage reports, the roster, main/alt mappings,
and adjustments for availability or raid timing. After saving changes, an officer
runs Pebble to update the published results.

## What about the Google Sheet?

The existing spreadsheet and its bot continue to work while officers transition
to the web app. Officers keep the inputs aligned manually during this period;
changes in one tool do not automatically appear in the other.

## Learn more

- [Attendance probabilities](Attendance_Probability.md): how the spreadsheet's
  attendance probability tables work, with explanations and formulas.
- [Web app development](apps/web/README.md): local setup, checks, and links to
  backend and deployment guides.
- [Sheets worker operations](docs/legacy-sheets-worker.md): running and maintaining
  the existing spreadsheet bot.
