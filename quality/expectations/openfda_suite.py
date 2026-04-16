import sys
import pandas as pd
import great_expectations as ge

SUITE_NAME = "openfda_suite"


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
    results.append(dataset.expect_column_values_to_not_be_null("drug_id"))
    results.append(dataset.expect_column_values_to_be_unique("drug_id"))
    results.append(dataset.expect_column_values_to_not_be_null("brand_name"))
    results.append(dataset.expect_column_values_to_not_be_null("generic_name"))
    results.append(dataset.expect_column_values_to_not_be_null("warnings"))

    failed = [r for r in results if not r.success]
    if failed:
        print("[FAIL] {} failed".format(SUITE_NAME))
        for r in failed:
            print("  - {}: {}".format(r.expectation_config.expectation_type, r.expectation_config.kwargs))
        raise SystemExit(1)

    print("[OK] {} passed".format(SUITE_NAME))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: openfda_suite.py <data_file_path>")
        raise SystemExit(2)
    run(sys.argv[1])
