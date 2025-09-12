from dataclasses import dataclass


@dataclass
class Product:
    ProductId: str
    Name: str
    Code: str
    Description: str
    IsActive: str
    CreatedAt: str
