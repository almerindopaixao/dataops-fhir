import re
from datetime import datetime
from typing import List
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def extract_family_name(full_name: List[str]):
    full_name_splited = full_name.split(" ")
    return " ".join(full_name_splited[1:]) if len(full_name_splited) > 1 else None


@udf(returnType=StringType())
def extract_given_name(full_name: List[str]):
    return full_name.split(" ")[0]


@udf(returnType=StringType())
def format_phone(phone: str):
    pattern = r"\((\d{2})\)\s(\d{4}-\d{4})"
    return re.sub(pattern, r"+55 0\1 \2", phone)


@udf(returnType=StringType())
def clean_cpf(value: str):
    return re.sub(r"\D", "", value)


@udf(returnType=StringType())
def format_gender(value: str):
    gender_clean = value.lower()

    if gender_clean == "masculino":
        return "male"

    if gender_clean == "feminino":
        return "female"

    return "unknown"


@udf(returnType=StringType())
def format_date(value: str):
    value_date = datetime.strptime(value, "%d/%m/%Y")
    return value_date.strftime("%Y-%m-%d")
