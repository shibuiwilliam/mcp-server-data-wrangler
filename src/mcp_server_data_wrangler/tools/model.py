import os
from abc import ABC
from enum import Enum

import polars as pl
from pydantic import BaseModel, Field


class SupportedFileType(Enum):
    csv = ("csv", ".csv")
    tsv = ("tsv", ".tsv")

    @staticmethod
    def from_str(file_type: str) -> "SupportedFileType":
        for ft in SupportedFileType:
            if ft.value[0] == file_type:
                return ft
        raise ValueError(f"Invalid file type: {file_type}")

    @staticmethod
    def from_extension(file_type: str) -> "SupportedFileType":
        for ft in SupportedFileType:
            if ft.value[1] == file_type:
                return ft
        raise ValueError(f"Invalid file type: {file_type}")

    @property
    def file_type(self) -> str:
        return self.value[0]

    @property
    def extension(self) -> str:
        return self.value[1]


class Data(BaseModel, ABC):
    df: pl.DataFrame = Field(description="DataFrame containing the data")

    @staticmethod
    def from_file(file_path: str) -> "Data":
        extension = os.path.splitext(file_path)[1]
        supported_file_type = SupportedFileType.from_str(extension)
        if supported_file_type == SupportedFileType.csv:
            return Data(df=pl.read_csv(file_path))
        elif supported_file_type == SupportedFileType.tsv:
            return Data(df=pl.read_csv(file_path, separator="\t"))
        raise ValueError(f"Unsupported file type: {extension}")
