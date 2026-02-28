import sys, traceback

# Add project source roots to import path
sys.path.insert(0, r'E:\src\horseless-airflow-dags\horseless-airflow-dags')
sys.path.insert(0, r'E:\src\horseless-repo-tracker\horseless-repo-tracker\src')

modules = [
    'horseless_airflow_dags.dags.github_ingester',
    'horseless_airflow_dags.dags.modelrun_starter',
    'horseless_repotracker.repotracker.redistransport.redis_transport',
    'horseless_repotracker.repotracker.orm',
    'horseless_repotracker.repotracker.dto.model_run_dto',
]

ok = True
for m in modules:
    try:
        __import__(m)
        print('OK import', m)
    except Exception:
        ok = False
        print('FAILED import', m)
        traceback.print_exc()

if not ok:
    raise SystemExit(2)
print('All imports succeeded')
