import os
import sys
import importlib.util

SUITE_FILES = {
    "faers": "faers_suite.py",
    "pubmed": "pubmed_suite.py",
    "reddit": "reddit_suite.py",
    "openfda": "openfda_suite.py",
}

EXPECTATIONS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "expectations")
)


def load_suite_module(suite_name):
    filename = SUITE_FILES[suite_name]
    path = os.path.join(EXPECTATIONS_DIR, filename)
    spec = importlib.util.spec_from_file_location("{}_module".format(suite_name), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    if len(sys.argv) < 3:
        print("Usage: run_checkpoint.py <suite_name> <data_file_path>")
        raise SystemExit(2)

    suite_name = sys.argv[1]
    data_file_path = sys.argv[2]

    if suite_name not in SUITE_FILES:
        print("[FAIL] Checkpoint failed for {}".format(suite_name))
        print("Unknown suite. Valid values: {}".format(", ".join(SUITE_FILES.keys())))
        raise SystemExit(1)

    module = load_suite_module(suite_name)
    try:
        module.run(data_file_path)
    except SystemExit as exc:
        if exc.code and exc.code != 0:
            print("[FAIL] Checkpoint failed for {}".format(suite_name))
            raise SystemExit(1)
    except Exception as exc:
        print("[FAIL] Checkpoint failed for {}".format(suite_name))
        print("Error: {}".format(exc))
        raise SystemExit(1)

    print("[OK] Checkpoint passed for {}".format(suite_name))


if __name__ == "__main__":
    main()
