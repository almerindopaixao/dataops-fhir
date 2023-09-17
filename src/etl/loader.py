import os
import json

from typing import List
from fhirpy import SyncFHIRClient
from fhir.resources.bundle import Bundle


class Loader:
    def __init__(self):
        fhir_url = os.getenv("FHIR_API_URL")
        self.__fhir_client = SyncFHIRClient(url=fhir_url)

    def run(self, bundles: List[Bundle]):
        for bundle in bundles:
            self.__fhir_client.resource("Bundle", **json.loads(bundle.json())).save()
