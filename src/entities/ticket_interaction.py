from dataclasses import dataclass

@dataclass
class TicketInteraction:
    InteractionId: str
    TicketId: str
    AuthorUserId: str
    AuthorAgentId: str
    InteractionType: str
    Message: str
    IsPublic: str
    CreatedAt: str
