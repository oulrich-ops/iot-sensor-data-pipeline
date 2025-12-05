import time
from pathlib import Path
from dotenv import load_dotenv

# Load env from project root
PROJECT_ROOT = Path(__file__).parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH, override=True)
else:
    print("Warning: .env not found at project root; relying on current environment")


def main():
    # Import here so modules can be used standalone too
    from spark_processing.utils import build_spark_session

    # Build one SparkSession with required packages
    spark = build_spark_session("spark_processing_orchestrator")

    # Import module start functions
    from spark_processing.alert_detector import start_alert_detector
    from spark_processing.data_persistance import start_data_persistance
    from spark_processing.data_agregator import start_data_agregator

    queries = []
    try:
        q1 = start_data_persistance(spark)
        if q1 is not None:
            queries.append(q1)

        q2 = start_alert_detector(spark)
        if q2 is not None:
            queries.append(q2)

        q3 = start_data_agregator(spark)
        if q3 is not None:
            queries.append(q3)

        print(f"Started {len(queries)} streaming queries. Waiting for termination (Ctrl+C to stop).")

        # Main loop: wait while any query is active
        while True:
            active = [q.isActive for q in queries if q is not None]
            if not any(active):
                print("No active queries remain; exiting")
                break
            time.sleep(1)

    except KeyboardInterrupt:
        print("Interrupted by user, stopping queries...")
    finally:
        for q in queries:
            try:
                if q is not None and q.isActive:
                    q.stop()
            except Exception:
                pass
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
