from fhir.resources.patient import Patient
from fhir.resources.humanname import HumanName
from fhir.resources.meta import Meta
from fhir.resources.identifier import Identifier
from fhir.resources.coding import Coding
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.contactpoint import ContactPoint
from fhir.resources.extension import Extension
from fhir.resources.observation import Observation
from fhir.resources.reference import Reference
from fhir.resources.bundle import Bundle, BundleEntry, BundleEntryRequest
from fhir.resources.resource import Resource

from pyspark.sql import Row


class Transformer:
    def __init__(self, patient: Row):
        self.__patient = patient

    def __create_patient_meta(self):
        meta = Meta()
        meta.profile = [
            "http://www.saude.gov.br/fhir/r4/StructureDefinition/BRIndividuo-1.0"
        ]

        return meta

    def __create_patient_identifier(self):
        identifier = Identifier()
        identifier.use = "official"

        coding = Coding()
        coding.system = (
            "http://www.saude.gov.br/fhir/r4/ValueSet/BRTipoDocumentoIndividuo-1.0"
        )
        coding.code = "TAX"

        code = CodeableConcept()
        code.coding = [coding]

        identifier.type = code
        identifier.value = self.__patient["patient_identifier_value"]

        return identifier

    def __create_patient_name(self):
        name = HumanName()

        name.use = "official"
        name.text = self.__patient["patient_name_text"]
        name.family = self.__patient["patient_name_family"]
        name.given = [self.__patient["patient_name_given"]]

        return name

    def __create_patient_telecom(self):
        telecom = ContactPoint()

        telecom.system = "phone"
        telecom.value = self.__patient["patient_telecom_value"]

        return telecom

    def __create_patient_birth_country(self):
        extension = Extension()
        extension.url = "http://www.saude.gov.br/fhir/r4/StructureDefinition/BRPais-1.0"

        coding = Coding()
        coding.system = "http://www.saude.gov.br/fhir/r4/CodeSystem/BRPais"
        coding.code = "10"

        code = CodeableConcept()
        code.coding = [coding]
        code.text = self.__patient["patient_birth_country_text"]

        extension.valueCodeableConcept = code

        return extension

    def __create_patient(self):
        fhir_patient = Patient()
        fhir_patient.id = self.__patient["patient_id"]

        fhir_patient.meta = self.__create_patient_meta()
        fhir_patient.identifier = [self.__create_patient_identifier()]

        fhir_patient.active = True

        fhir_patient.name = [self.__create_patient_name()]
        fhir_patient.telecom = [self.__create_patient_telecom()]

        fhir_patient.gender = self.__patient["patient_gender"]
        fhir_patient.birthDate = self.__patient["patient_birth_date"]

        fhir_patient.extension = [self.__create_patient_birth_country()]

        return fhir_patient

    def __create_observation(self, observation_text: str):
        coding = Coding()
        coding.system = "https://loinc.org"

        if observation_text == "Gestante":
            coding.code = "82810-3"
            coding.display = "Pregnancy status"
        elif observation_text == "Diabético":
            coding.code = "33248-6"
            coding.display = "Diabetes status [Identifier]"
        elif observation_text == "Hipertenso":
            coding.code = "45643-4"
            coding.display = "Hypertension [Minimum Data Set]"

        code = CodeableConcept()
        code.coding = [coding]

        reference = Reference()
        reference.reference = f"Patient/{self.__patient['patient_id']}"

        return Observation(code=code, status="final", subject=reference)

    def __create_resource_entry(self, resource: Resource, resource_type: str):
        return BundleEntry(
            resource=resource,
            request=BundleEntryRequest(method="POST", url=resource_type),
        )

    def create_bundle(self):
        bundle = Bundle(type="transaction")
        bundle_entrys = []

        fhir_patient = self.__create_patient()

        bundle_entrys.append(
            self.__create_resource_entry(resource=fhir_patient, resource_type="Patient")
        )

        for observation in self.__patient["observations"]:
            fhir_observation = self.__create_observation(observation_text=observation)

            bundle_entrys.append(
                self.__create_resource_entry(
                    resource=fhir_observation, resource_type="Observation"
                )
            )

        bundle.entry = bundle_entrys

        return bundle
