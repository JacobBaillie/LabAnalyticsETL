<img width="1100" height="400" alt="Productivity_weekly_30DMA" src="https://github.com/user-attachments/assets/c925e782-a372-46e5-be53-d11a228fd85d" />

Laboratory Scheduling Analytics ETL

Our lab was facing experiment backlogs, sometimes forcing users to wait over a month. We knew there were scheduling conflicts, overbooked equipment, and uneven usage, but not a clear solution. We suspected that poor planning and communication were causing underutilization. The instrument booking system is Google Calendar. The user is only tracked if they include their name in the booking. Instrument utilization is not tracked, but a shared file storage system is universally used. 

Goal: Reduce experiment lead-times by better utilizing instruments. Provide statistical credibility to enforce administrative changes.

Pulled events from shared calendar via Google Calendar API.
De-identified all sensitive fields using HMAC hashing before storage.
Normalized timezones and recurring events.
Engineered behavioral features (booking details, lead time, duration, after-hours usage, and last-minute changes).
Scanned shared lab drives to measure daily activity per user folder (number of files created per day per user).
Corrected timestamp drift and filtered to valid experiment booking windows by matching the user to the instrument booked each day (matching user’s name in reservation and file storage folder).
Implemented batching logic to fix outliers (some experiments generate one file per datum while others produce one file per experiment).
Loaded and organized data in PostgreSQL for high-level EDA. Manually confirmed outliers are real data.
Used generalize linear model (negative binomial) to determine title length as a crucial predictor for file count.
Used univariate regression, then multivariate regression with extraneous variables (weekday, event duration, lead time) to ensure confidence in assessment.
Policy was implemented at start of 2025.

One year later, performed 3 causal regression models (interrupted time series) to confirm:
File counts are higher in 2025 based on Model A (total effect considering policy only).
Higher utilization is strongly linked to more detailed reservation info, quantified by Model B (association model).
Improved reservation details account for most of the increased file counts after the policy is created based on Model C, indicating the policy had the intended effect on user behavior.

---
Policy impact
---
Before policy change: 21.65 files per use

After policy change: 33.26 files per use

Actual change: 53.7%

_e_<sup>_β_</sup> - 1 = +50.9%

Remaining change is due to day of week, number of users, event duration, scheduling lead time


Models used
---

Model A — Total policy effect (preferred causal estimand)

>Purpose: estimate the total effect of the policy without conditioning on policy-induced mediators.

>Conclusion: policy change accounts for 50.9% increase in instrument utilization


Model B — Documentation/mechanism association

>Purpose: test whether documented features correlate with productivity

>Conclusion: more detailed event descriptions lead to better instrument utilization (0.25% per character). Mentioning the light soruce massively predicts better utilization (+140%)


Model C — Mediation decomposition (direct effect)

>Purpose: estimate the policy effect net of documentation variables (i.e., the “direct” component after accounting for the title/metadata channel).

>Conclusion: most of the increase in utilization following policy change is due to increased title length + more frequent mention of light soruce (47.2% increase due to these factors)

Notes
---
-  Dispersion = Var/Mean = 49 (extremely overdispersed)

      Only 8.7% of events result in 0 or 1 files; dispersion is due to high file-count burst days

-  Negative Binomial GLM used for fitting

-  Data scarcity in 2020 is due to COVID-19 shutdowns. Subsequent surge is due to increased pressure due to backlog.
