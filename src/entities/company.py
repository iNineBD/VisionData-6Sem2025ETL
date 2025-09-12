from dataclasses import dataclass


@dataclass
class Company:
    CompanyId: str
    Name: str
    CNPJ: str
    Segmento: str
    CreatedAt: str
