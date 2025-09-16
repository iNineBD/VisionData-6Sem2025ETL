from dataclasses import dataclass


@dataclass
class TicketStatusHistory:
    HistoryId: str
    TicketId: str
    FromStatusId: str
    ToStatusId: str
    ChangedAt: str
    ChangedByAgentId: str
