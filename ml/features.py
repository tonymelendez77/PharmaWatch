import numpy as np
import pandas as pd

FEATURE_COLUMNS = [
    "drug_name",
    "age_group",
    "adv_avg_age",
    "adv_serious_reports",
    "adv_total_reports",
    "sent_total_mentions",
    "sent_avg_score",
    "res_total_papers",
    "lbl_warnings_length",
    "lbl_has_interactions",
]

CATEGORICAL_COLUMNS = ["drug_name", "age_group"]

CONTINUOUS_COLUMNS = [
    "adv_avg_age",
    "adv_serious_reports",
    "adv_total_reports",
    "sent_total_mentions",
    "sent_avg_score",
    "res_total_papers",
    "lbl_warnings_length",
    "lbl_has_interactions",
]


def derive_age_group(age):
    if age is None:
        return "unknown"
    try:
        if isinstance(age, float) and np.isnan(age):
            return "unknown"
        age_int = int(age)
    except (TypeError, ValueError):
        return "unknown"
    if age_int < 18:
        return "0-17"
    if age_int < 35:
        return "18-34"
    if age_int < 65:
        return "35-64"
    return "65+"


def _apply_dtypes(frame):
    for col in CATEGORICAL_COLUMNS:
        frame[col] = frame[col].astype("category")
    for col in CONTINUOUS_COLUMNS:
        frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(-1)
    return frame


def build_features(df):
    df = df.copy()
    df = df[df["adv_total_reports"].astype(float) > 0].reset_index(drop=True)

    total = df["adv_total_reports"].astype(float)
    y = {
        "y_serious": (df["adv_serious_reports"].astype(float) / total).values,
        "y_hospitalization": (df["adv_hospitalization_reports"].astype(float) / total).values,
        "y_death": (df["adv_death_reports"].astype(float) / total).values,
        "y_disability": (df["adv_disability_reports"].astype(float) / total).values,
    }

    if "age_group" not in df.columns:
        df["age_group"] = df["adv_avg_age"].apply(derive_age_group)

    X = df[FEATURE_COLUMNS].copy()
    X = _apply_dtypes(X)

    return X, y


def build_user_profile(drug_name, age, weight, master_df):
    match = master_df[master_df["drug_name"] == drug_name]
    if match.empty:
        return None
    row = match.iloc[0]

    def _get(col):
        return row[col] if col in master_df.columns else None

    profile = {
        "drug_name": drug_name,
        "age_group": derive_age_group(age),
        "adv_avg_age": _get("adv_avg_age"),
        "adv_serious_reports": _get("adv_serious_reports"),
        "adv_total_reports": _get("adv_total_reports"),
        "sent_total_mentions": _get("sent_total_mentions"),
        "sent_avg_score": _get("sent_avg_score"),
        "res_total_papers": _get("res_total_papers"),
        "lbl_warnings_length": _get("lbl_warnings_length"),
        "lbl_has_interactions": _get("lbl_has_interactions"),
    }

    out = pd.DataFrame([profile], columns=FEATURE_COLUMNS)
    out = _apply_dtypes(out)
    return out
