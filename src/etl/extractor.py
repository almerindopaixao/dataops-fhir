from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, split, when, expr

from src.pyspark.udfs import (
    extract_family_name,
    extract_given_name,
    format_phone,
    format_date,
    format_gender,
    clean_cpf,
)


class Extractor:
    def __init__(self, process: int):
        self.__spark_session = (
            SparkSession.builder.master(f"local[{process}]")
            .appName("DataOpsFHIR")
            .getOrCreate()
        )

    def __read_patients(self):
        return self.__spark_session.read.csv(
            path="./data/patients.csv", header=True, sep=",", encoding="utf-8"
        )

    def __clean_patients(self, df: DataFrame):
        return (
            df.withColumn("patient_id", clean_cpf(col("CPF")))
            .withColumnRenamed("CPF", "patient_identifier_value")
            .withColumnRenamed("Nome", "patient_name_text")
            .withColumn(
                "patient_name_given", extract_given_name(col("patient_name_text"))
            )
            .withColumn(
                "patient_name_family", extract_family_name(col("patient_name_text"))
            )
            .withColumn("patient_telecom_value", format_phone(col("Telefone")))
            .withColumn("patient_gender", format_gender(col("Gênero")))
            .withColumn("patient_birth_date", format_date(col("Data de Nascimento")))
            .withColumnRenamed("País de Nascimento", "patient_birth_country_text")
            .withColumn(
                "observations",
                when(col("Observação").isNull(), expr("array()")).otherwise(
                    split(col("Observação"), "\\|")
                ),
            )
        )

    def run(self):
        df_patients = self.__read_patients()
        df_patients = self.__clean_patients(df_patients)

        return df_patients.collect()
