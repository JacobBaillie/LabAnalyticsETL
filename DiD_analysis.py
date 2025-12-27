"""
Load your productivity_table view from Postgres, deduplicate rare multiple-event person-days,
create a pre/post policy indicator, and fit a Negative Binomial GLM with day-of-week controls.
"""

# ============================
# User inputs (edit these)
# ============================
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "lab_analytics"
DB_USER = "postgres"
DB_PASSWORD = "***" # <-- CHANGE ME

VIEW_NAME = "productivity_table"   # your SQL VIEW
DATE_MIN = "2020-01-01"
DATE_MAX = "2026-01-01"

# Intervention (policy change) date
POLICY_CHANGE_DATE = "2025-01-01"  # <-- change to your real date

# Dedup rule for rare duplicates on same (person/day):
# "max_duration" keeps the longest event per person-day
DEDUP_RULE = "max_duration"  # options: "max_duration", "first", "last"

# Output path
want_export = False
OUTPUT_CSV = r"C:\Users\Jacob\Dropbox\Python\Lab Analytics\outputs\productivity_model_clean.csv"

# ============================
# Imports
# ============================
import os
import numpy as np
import pandas as pd
import psycopg2
import statsmodels.api as sm
import statsmodels.formula.api as smf
from statsmodels.stats.sandwich_covariance import cov_cluster
from patsy import dmatrices
import matplotlib.pyplot as plt
import re

# ============================
# Load from Postgres
# ============================
conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)

query = f"""
SELECT *
FROM {VIEW_NAME}
WHERE date >= %s AND date < %s
"""
df = pd.read_sql(query, conn, params=[DATE_MIN, DATE_MAX])

print("Rows loaded:", len(df))
print("Min date loaded:", pd.to_datetime(df["date"]).min())
print("Max date loaded:", pd.to_datetime(df["date"]).max())

# Count rows by year
tmp = pd.to_datetime(df["date"], errors="coerce")
print(tmp.dt.year.value_counts().sort_index())

conn.close()

# ============================
# Basic cleaning / types
# ============================
# Columns list:
# event_title, date, day_of_week, event_start_date, event_end_date, event_duration,
# lead_time_hr, title_length, mentions_wavelength_lightsource, mother_folder, file_count
df["date"] = pd.to_datetime(df["date"]).dt.date
df["mother_folder"] = df["mother_folder"].astype(str)
df["file_count"] = pd.to_numeric(df["file_count"], errors="coerce").fillna(0).astype(int)

# event_duration might be NULL; ensure numeric
if "event_duration" in df.columns:
    df["event_duration"] = pd.to_numeric(df["event_duration"], errors="coerce").fillna(0.0)
else:
    df["event_duration"] = 0.0

# Derive day-of-week from date (holistic control, consistent with file-day)
df["day_of_week"] = pd.to_datetime(df["date"]).dt.day_name()

# Policy indicator
policy_date = pd.to_datetime(POLICY_CHANGE_DATE).date()
df["is_post_policy"] = (df["date"] >= policy_date).astype(int)

# Lead time: remove non-physical negatives (common with edits/imports) without clipping
if "lead_time_hr" in df.columns:
    df["lead_time_hr"] = pd.to_numeric(df["lead_time_hr"], errors="coerce")
    df["lead_time_hr_clean"] = df["lead_time_hr"].where(df["lead_time_hr"] >= 0, np.nan)
    med = df["lead_time_hr_clean"].median()
    df["lead_time_hr_clean"] = df["lead_time_hr_clean"].fillna(med if np.isfinite(med) else 0.0)
else:
    df["lead_time_hr_clean"] = 0.0

# Title length
def semantic_title_length(s):
    if not isinstance(s, str):
        return 0
    parts = re.split(r"\s+", s.strip())
    if len(parts) <= 1:
        return 0
    payload = " ".join(parts[1:])   # drop first word
    return len(payload)

df["title_len"] = df["event_title"].apply(semantic_title_length).astype(float)


# Mentions flag
if "mentions_wavelength_lightsource" in df.columns:
    df["mentions_wave"] = pd.to_numeric(df["mentions_wavelength_lightsource"], errors="coerce").fillna(0).astype(int)
else:
    df["mentions_wave"] = 0

# ============================
# Deduplicate rare multi-event days (person-day)
# ============================
df = df.rename(columns={"mother_folder": "person", "file_count": "files"})

if DEDUP_RULE == "max_duration":
    df = df.sort_values(["person", "date", "event_duration"], ascending=[True, True, False])
    df = df.drop_duplicates(subset=["person", "date"], keep="first")
elif DEDUP_RULE == "first":
    df = df.sort_values(["person", "date", "event_start_date"], ascending=[True, True, True])
    df = df.drop_duplicates(subset=["person", "date"], keep="first")
elif DEDUP_RULE == "last":
    df = df.sort_values(["person", "date", "event_start_date"], ascending=[True, True, True])
    df = df.drop_duplicates(subset=["person", "date"], keep="last")
else:
    raise ValueError("DEDUP_RULE must be one of: max_duration, first, last")

# ============================
# Export cleaned dataset (optional)
# ============================
if want_export == True:
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)

# ============================
# Sanity checks
# ============================

print("Rows after dedup:", len(df))
print("Unique person-days:", df[["person", "date"]].drop_duplicates().shape[0])
print("Mean files pre:", df.loc[df["is_post_policy"] == 0, "files"].mean())
print("Mean files post:", df.loc[df["is_post_policy"] == 1, "files"].mean())
print("Var(files) / Mean(files):", (df["files"].var(ddof=1) / max(df["files"].mean(), 1e-9)))
print("Zero-files and One-file fraction:", (df["files"] < 2).mean())

# ============================
# Fit function
# ============================
def fit_nb_cluster(formula, df, group_col="person", maxiter=500, method="lbfgs", disp=True):
    y, X = dmatrices(formula, data=df, return_type="dataframe")
    y = y.iloc[:, 0]

    res = sm.NegativeBinomial(y, X).fit(method=method, maxiter=maxiter, disp=disp)

    # cluster-robust covariance (version-safe)
    res.cov_params_default = cov_cluster(res, df[group_col])
    return res


# ============================
# Model A — True policy effect; does the policy improve productivity?
# ============================
# Day-of-week included holistically via C(day_of_week)
# 'C' indicates categorical variable
formula_policy = """
files ~
    is_post_policy
  + event_duration
  + lead_time_hr_clean
  + mentions_wave
  + C(day_of_week)
"""

yA, XA = dmatrices(formula_policy, data=df, return_type="dataframe")
yA = yA.iloc[:, 0]
res_policy = fit_nb_cluster(formula_policy, df, group_col="person", method="lbfgs", maxiter=500, disp=True)
print(res_policy.summary())

# ============================
# Model B — Documentation design effect; does the title length matter?
# ============================
formula_title = """
files ~
    title_len
  + event_duration
  + lead_time_hr_clean
  + mentions_wave
  + C(day_of_week)
"""

yB, XB = dmatrices(formula_title, data=df, return_type="dataframe")
yB = yB.iloc[:, 0]
res_title = fit_nb_cluster(formula_title, df, group_col="person", method="lbfgs", maxiter=500, disp=True)
print(res_title.summary())


# ============================
# Model C — Mediation decomposition; how much does title length mediate the policy effect?
# ============================
formula_mediation = """
files ~
    is_post_policy
  + title_len
  + event_duration
  + lead_time_hr_clean
  + mentions_wave
  + C(day_of_week)
"""

yC, XC = dmatrices(formula_mediation, data=df, return_type="dataframe")
yC = yC.iloc[:, 0]
res_mediation = fit_nb_cluster(formula_mediation, df, group_col="person", method="lbfgs", maxiter=500, disp=True)
print(res_mediation.summary())

# ============================
# Convert coefficients to percent effects (A, B, C separately)
# ============================
def coef_to_pct_table(res, label):
    coef = pd.Series(res.params, index=res.model.exog_names)
    pct = (np.exp(coef) - 1.0) * 100.0
    out = pd.DataFrame({"coef": coef, "pct_change_%": pct})
    out["model"] = label
    return out.sort_values("pct_change_%", ascending=False)

effects_A = coef_to_pct_table(res_policy, "A_total_policy")
effects_B = coef_to_pct_table(res_title, "B_title_effect")
effects_C = coef_to_pct_table(res_mediation, "C_direct_policy")

print("\n--- Multiplicative effects (% change), Model A ---")
print(effects_A)

print("\n--- Multiplicative effects (% change), Model B ---")
print(effects_B)

print("\n--- Multiplicative effects (% change), Model C ---")
print(effects_C)

# ============================
# Policy effect decomposition: total vs direct (A vs C)
# ============================
betaA = float(res_policy.params[list(XA.columns).index("is_post_policy")] if hasattr(res_policy.params, "__len__") else res_policy.params["is_post_policy"])
betaC = float(res_mediation.params[list(XC.columns).index("is_post_policy")] if hasattr(res_mediation.params, "__len__") else res_mediation.params["is_post_policy"])

total_pct = (np.exp(betaA) - 1.0) * 100.0
direct_pct = (np.exp(betaC) - 1.0) * 100.0

print("\nPolicy effect at baseline covariates (reference weekday, interactions excluded):")
print(f"  Total policy effect (Model A):  {total_pct:.2f}%")
print(f"  Direct policy effect (Model C): {direct_pct:.2f}%")

# Approx mediated share on log scale (more stable than percent space)
# mediated_share = (betaA - betaC) / betaA   (guard against betaA≈0)
if abs(betaA) > 1e-9:
    mediated_share = (betaA - betaC) / betaA
    print(f"  Approx mediated share via title_len: {mediated_share:.3f}")
else:
    print("  Approx mediated share: undefined (total policy effect ~ 0)")

# ============================
# Plot: weekly mean bars + 30-day moving average
# ============================

# Seattle-local dates
dt = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.tz_convert("America/Los_Angeles")
df["_date_local"] = dt.dt.date
df["_week"] = dt.dt.to_period("W-MON").dt.start_time

# Weekly mean bars
wk = (
    df.groupby("_week")["files"]
      .mean()
      .reset_index()
      .rename(columns={"_week": "week", "files": "mean_files"})
)

# 30-day moving average (daily resolution)
daily = (
    df.groupby("_date_local")["files"]
      .mean()
      .reset_index()
      .rename(columns={"_date_local": "date", "files": "mean_files"})
)

daily["date"] = pd.to_datetime(daily["date"])
daily = daily.sort_values("date")
daily["ma30"] = daily["mean_files"].rolling(30, min_periods=10).mean()

# Plot
plt.figure(figsize=(11,4))
plt.bar(wk["week"], wk["mean_files"], width=5, alpha=0.35)
plt.plot(daily["date"], daily["ma30"], linewidth=2)
plt.axvline(pd.to_datetime(POLICY_CHANGE_DATE), linestyle="--")
plt.xlabel("Week / Date")
plt.xlim(pd.to_datetime('2020'), pd.to_datetime('2026'))
plt.ylabel("Files per day (weekly average)")
plt.title("Weekly productivity with 30-day moving average ignoring unused days")
plt.tight_layout()
plt.show()
