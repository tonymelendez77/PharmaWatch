import sys
import pandas as pd
import great_expectations as ge

SUITE_NAME = "faers_suite"


def load_dataframe(path):
    try:
        return pd.read_json(path, lines=True)
    except ValueError:
        return pd.read_json(path)


def run(path):
    df = load_dataframe(path)
    dataset = ge.from_pandas(df)
    dataset._expectation_suite.expectation_suite_name = SUITE_NAME

    results = []
    results.append(dataset.expect_column_values_to_not_be_null("report_id"))
    results.append(dataset.expect_column_values_to_be_unique("report_id"))
    results.append(dataset.expect_column_values_to_not_be_null("drug_name"))
    results.append(dataset.expect_column_values_to_not_be_null("severity"))
    results.append(dataset.expect_column_values_to_be_in_set("severity", [1, 2]))
    results.append(dataset.expect_column_values_to_be_between("age", min_value=0, max_value=120, mostly=1.0))
    results.append(dataset.expect_column_values_to_be_between("weight", min_value=0, max_value=500, mostly=1.0))
    results.append(dataset.expect_column_values_to_not_be_null("report_date"))
    results.append(dataset.expect_column_values_to_match_regex("report_date", r"^\d{8}$"))

    failed = [r for r in results if not r.success]
    if failed:
        print("[FAIL] {} failed".format(SUITE_NAME))
        for r in failed:
            print("  - {}: {}".format(r.expectation_config.expectation_type, r.expectation_config.kwargs))
        raise SystemExit(1)

    print("[OK] {} passed".format(SUITE_NAME))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: faers_suite.py <data_file_path>")
        raise SystemExit(2)
    run(sys.argv[1])
