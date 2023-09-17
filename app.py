import os
import time
import requests

from src.etl import Extractor, Transformer, Loader


fhir_url = os.getenv("FHIR_API_URL")


def fhir_server_health_check():
    check_interval_seconds = 30
    max_checks = 10

    for count in range(max_checks):
        try:
            response = requests.get(
                f"{fhir_url}/metadata", timeout=check_interval_seconds
            )

            if 200 <= response.status_code < 300:
                print(f"VERIFY {count + 1}: FHIR SERVER AVAILABLE.")
                return

            print(f"VERIFY {count + 1}: FHIR SERVER UNAVAILABLE.")
        except requests.ConnectionError:
            print(f"VERIFY {count + 1}: FHIR SERVER UNAVAILABLE.")
        except Exception as error:
            print("ERROR DURING HEALTH CHECK FHIR SERVER", error)
            raise error
        finally:
            if count < max_checks - 1:
                time.sleep(check_interval_seconds)

    print("VERIFY FAILED: FHIR SERVER UNAVAILABLE.")


def transform_patient(data):
    transformer = Transformer(patient=data)
    return transformer.create_bundle()


def main():
    try:
        extractor = Extractor(process=4)
        patients = extractor.run()

        bundles = list(map(transform_patient, patients))

        loader = Loader()
        loader.run(bundles=bundles)
    except Exception as error:
        print("ERROR DURING EXECUTION: ", error)


if __name__ == "__main__":
    fhir_server_health_check()
    main()
