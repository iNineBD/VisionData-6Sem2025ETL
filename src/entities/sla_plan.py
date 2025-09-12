from dataclasses import dataclass

@dataclass
class SLAPlan:
    SLAPlanId: str
    Name: str
    FirstResponseMins: str
    ResolutionMins: str
