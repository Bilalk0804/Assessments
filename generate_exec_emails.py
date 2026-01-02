import pandas as pd
import re
from pathlib import Path


# -----------------------------
# CONFIG
# -----------------------------
# Get the script's directory and create paths relative to it
SCRIPT_DIR = Path(__file__).parent
INPUT_FILE = SCRIPT_DIR / "input" / "cmo_videos_names.xlsx"
OUTPUT_FILE = SCRIPT_DIR / "output" / "senior_exec_emails.csv"
MAX_EXECUTIVES = 50


# -----------------------------
# STEP 1: LOAD DATA
# -----------------------------
def load_data(file_path: Path) -> pd.DataFrame:
    """
    Load Excel file and retain only relevant columns.
    """
    df = pd.read_excel(file_path)
    df = df[["Name", "Title", "Company"]].dropna()
    return df


# -----------------------------
# STEP 2: FILTER SENIOR EXECUTIVES
# -----------------------------
def filter_senior_execs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only senior executive roles.
    """
    senior_keywords = [
        "chief", "ceo", "cmo", "cto", "cfo", "coo",
        "president", "founder", "co-founder", "evp", "svp"
    ]

    df["is_senior"] = df["Title"].str.lower().apply(
        lambda title: any(keyword in title for keyword in senior_keywords)
    )

    senior_df = df[df["is_senior"]].copy()
    senior_df.drop(columns=["is_senior"], inplace=True)
    return senior_df


# -----------------------------
# STEP 3: NORMALIZE NAMES
# -----------------------------
def split_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split full name into first and last name.
    """
    name_split = df["Name"].str.strip().str.split(" ", n=1, expand=True)
    df["FirstName"] = name_split[0]
    df["LastName"] = name_split[1]
    return df.dropna(subset=["LastName"])


# -----------------------------
# STEP 4: INFER EMAIL DOMAIN
# -----------------------------
def infer_domain(company: str) -> str:
    """
    Convert company name to a likely email domain.
    Example: 'World Surf League' -> 'worldsurfleague.com'
    """
    cleaned = re.sub(r"[^a-zA-Z0-9 ]", "", company)
    domain = cleaned.lower().replace(" ", "")
    return f"{domain}.com"


def add_domains(df: pd.DataFrame) -> pd.DataFrame:
    df["Domain"] = df["Company"].apply(infer_domain)
    return df


# -----------------------------
# STEP 5: GENERATE EMAILS
# -----------------------------
def generate_emails(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate two high-probability enterprise email formats.
    """
    df["Email_1"] = (
        df["FirstName"].str.lower()
        + "."
        + df["LastName"].str.lower()
        + "@"
        + df["Domain"]
    )

    df["Email_2"] = (
        df["FirstName"].str.lower()
        + "@"
        + df["Domain"]
    )

    return df


# -----------------------------
# STEP 6: PIPELINE EXECUTION
# -----------------------------
def main():
    # Load
    df = load_data(INPUT_FILE)

    # Filter senior executives
    df = filter_senior_execs(df)

    # Normalize names
    df = split_names(df)

    # Infer domains
    df = add_domains(df)

    # Generate emails
    df = generate_emails(df)

    # Select final columns
    final_df = df[
        ["FirstName", "LastName", "Title", "Company", "Email_1", "Email_2"]
    ].head(MAX_EXECUTIVES)

    # Ensure output directory exists
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Export
    final_df.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Generated {len(final_df)} senior executive email records")
    print(f"📁 Output saved to: {OUTPUT_FILE}")


# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    main()
