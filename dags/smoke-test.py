import pendulum
from airflow.sdk import dag, task


@dag(schedule=None, start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
     catchup=False, tags=["smoke-test"])
def smoke_test():
    @task
    def check_environment():
        import subprocess
        from src import config           # fails here if the code mount or PYTHONPATH is wrong

        java = subprocess.run(["java", "-version"], capture_output=True, text=True)
        print(java.stderr.strip())        # java prints its version on stderr
        print("landing dir exists:", config.LANDING_DIR, config.LANDING_DIR.exists())

        config.RAW_DIR.mkdir(parents=True, exist_ok=True)
        probe = config.RAW_DIR / ".write_test"
        probe.write_text("ok")            # fails here on a permissions problem
        probe.unlink()
        print("raw dir is writable:", config.RAW_DIR)

    check_environment()


smoke_test()