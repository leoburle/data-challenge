# Data Engineering — Technical Assignment

The goal of this assignment is:
- **For you:** To see what type of issues you will be expected to work on at Helu.
- **For us:** To see how you think and approach tasks.

## Problem Statement

A SaaS company sells its app on two different platforms. The CFO needs a financial report that tracks metrics for both platforms (including acquisitions, renewals, and MRR), for each country per month. Currently, the CFO has to analyse the financial reports on each platform separately, which is too time-consuming!

The Data Engineering team is tasked with ingesting subscription events from each platform and outputting a consolidated report. The Analytics team will then build a visualisation for the CFO on top of it.

> **MRR Definition:** The total monthly revenue expected from active subscriptions at the end of each reporting period. Cancelled subscriptions should not contribute to MRR.

---

## Requirements

- Your pipeline should evaluate and fix any data quality issues you identify.
- The consolidated report must include at least the following attributes: `platform`, `subscription_type`, `country`, `acquisitions`, `renewals`, `cancellations`, `mrr_eur`.
- Subscription events and exchange rates should be ingested from the Docker-based API (see [Data Sources](#data-sources) below).
- The report should be stored in a **queryable format** (please document reasons for your storage decision).
- The pipeline should be **idempotent**.

---

## Data Sources

### Starting the Subscription API

Run the following command from the project root to build and start the data API:

```bash
docker compose up --build
```

Verify it's running:
```bash
curl http://localhost:5050/health
```

Once running, the following endpoints are available at `http://localhost:5050`:

| Endpoint | Format | Description |
|----------|--------|-------------|
| `/apfel/subscriptions` | JSON | Apfel platform subscription events |
| `/fenster/subscriptions` | CSV | Fenster platform subscription events |
| `/exchange-rates` | CSV | Exchange rates |
| `/health` | JSON | Health check |

> **Note:** There is no provided schema documentation for these endpoints. Data exploration is part of the exercise.

> If you have problems running or setting up Docker, you can use the CSV source files in the `data/` directory directly instead.

---

## Deliverables

1. **An executable Python (3.10+) solution** that meets all the above requirements.
2. **A schema for the report** should be defined — consider what would make the report meaningful and useful for the CFO.
3. **A README file** that explains:
   - How to run the solution (and any setup required)
   - How to query the report
   - Design decisions you made along the way

Please fork this repository and submit your solution as a link to your fork. You are free to use any libraries you prefer.

### Stretch Goals

These are optional — only if you have time and want to go further:

- Add validation or quality checks on the final report
- Write meaningful tests

---

## Guidelines

- Think of this assessment as a real project at Helu.
- Ensure your code is of **high quality** and follows best design practices. However, there is no need to implement a production-ready, infinitely scalable solution (running the solution locally is sufficient).
- **Quality over completeness.** We would rather see a well-thought-out partial solution than a complete one you can't fully explain. If you run out of time, document your thinking and next steps in the README.
- **Time expectation:** The solution should take around **3 hours**. If it takes longer for any reason, or you don't have enough time for all deliverables, that's no problem! Focus on doing good work on the parts you do complete.
- The problem is intentionally vaguely defined (e.g. what storage to use) — we're interested in how you approach problems and make decisions with limited information.
- **We evaluate understanding over output.** You should be able to explain every part of your code and design choices in a follow-up interview.

Good luck and have fun!
