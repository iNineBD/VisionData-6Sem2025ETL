from dataclasses import dataclass


@dataclass
class AuditLog:
    AuditId: str
    EntityType: str
    EntityId: str
    Operation: str
    PerformedBy: str
    PerformedAt: str
    DetailsJson: str
