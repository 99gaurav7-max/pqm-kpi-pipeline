import os
import pandas as pd

SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQVPQQktteGtbHuolupOKRP0Bagu_5RJiVBh9ziUG89lsTdBZ7euRdsh2LGg77qc0zX2he7wGFvajru/pub?gid=0&single=true&output=csv"

# ---- Column names (single-line versions) ----
COL_PQM = "Name of the PQM"
COL_STATE = "State"
COL_SCHEME = "Scheme"
COL_ALLOTTED = "Alloted in Vikas Setu Portal (Yes/No)"
COL_REPORT = "Report Submission in Vikas Setu Portal (Yes/No)"
COL_SCHED = "Scheduled Date of Visit"
COL_ACTUAL = "Actual Date of Inspection"
COL_PROJECT_ID = "Project ID"

def normalize_yes_no(x):
    if pd.isna(x):
        return ""
    x = str(x).strip().upper()
    if x in ["YES", "Y", "TRUE", "1"]:
        return "YES"
    if x in ["NO", "N", "FALSE", "0"]:
        return "NO"
    if x in ["N/A", "NA"]:
        return "NA"
    return x

def main():
    df = pd.read_csv(SHEET_CSV_URL)

    # critical: remove \n etc. in headers
    df.columns = [" ".join(str(c).split()) for c in df.columns]

    required = [COL_PQM, COL_STATE, COL_SCHEME, COL_ALLOTTED, COL_REPORT, COL_SCHED, COL_ACTUAL, COL_PROJECT_ID]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print("Available columns:")
        print(df.columns.tolist())
        raise ValueError(f"Missing columns in sheet: {missing}")

    # Normalize Yes/No fields
    df[COL_ALLOTTED] = df[COL_ALLOTTED].apply(normalize_yes_no)
    df[COL_REPORT] = df[COL_REPORT].apply(normalize_yes_no)

    # Parse dates (dd-mm-yyyy)
    df[COL_SCHED] = pd.to_datetime(df[COL_SCHED], errors="coerce", dayfirst=True)
    df[COL_ACTUAL] = pd.to_datetime(df[COL_ACTUAL], errors="coerce", dayfirst=True)

    # Month-Year based on Scheduled Date
    df["MonthYear"] = df[COL_SCHED].dt.to_period("M").astype(str)

    # Flags
    df["IsAssigned"] = (df[COL_ALLOTTED] == "YES").astype(int)
    df["IsReportSubmitted"] = (df[COL_REPORT] == "YES").astype(int)
    df["IsInspected"] = df[COL_ACTUAL].notna().astype(int)

    # KPI table
    kpi = (
        df.groupby([COL_PQM, COL_SCHEME, COL_STATE, "MonthYear"], dropna=False)
          .agg(
              TotalProjects=("IsAssigned", "size"),
              AssignedProjects=("IsAssigned", "sum"),
              InspectedProjects=("IsInspected", "sum"),
              ReportsSubmitted=("IsReportSubmitted", "sum"),
              UniqueProjects=(COL_PROJECT_ID, "nunique"),
          )
          .reset_index()
    )

    # PQM totals
    pqm_totals = (
        df.groupby([COL_PQM], dropna=False)
          .agg(
              TotalRows=("IsAssigned", "size"),
              UniqueProjects=(COL_PROJECT_ID, "nunique"),
              AssignedProjects=("IsAssigned", "sum"),
              InspectedProjects=("IsInspected", "sum"),
              ReportsSubmitted=("IsReportSubmitted", "sum"),
          )
          .reset_index()
    )

    # Output folder for GitHub
    out_dir = "output"
    os.makedirs(out_dir, exist_ok=True)

    df.to_csv(os.path.join(out_dir, "gold_fact_projects.csv"), index=False)
    kpi.to_csv(os.path.join(out_dir, "kpi_pqm_scheme_state_month.csv"), index=False)
    pqm_totals.to_csv(os.path.join(out_dir, "kpi_pqm_totals.csv"), index=False)

    print("✅ Exported to ./output")

if __name__ == "__main__":
    main()
