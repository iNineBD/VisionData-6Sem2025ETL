from dataclasses import dataclass


@dataclass
class User:
    UserId: str
    CompanyId: str
    FullName: str
    Email: str
    Phone: str
    CreatedAt: str
    IsVIP: str
