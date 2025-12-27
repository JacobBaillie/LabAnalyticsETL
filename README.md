<img width="1100" height="400" alt="Productivity_weekly_30DMA" src="https://github.com/user-attachments/assets/c925e782-a372-46e5-be53-d11a228fd85d" />

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
