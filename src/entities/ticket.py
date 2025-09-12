from dataclasses import dataclass

@dataclass
class Ticket:
    TicketId: str
    CompanyId: str
    CreatedByUserId: str
    AssignedAgentId: str
    ProductId: str
    CategoryId: str
    SubcategoryId: str
    PriorityId: str
    CurrentStatusId: str
    SLAPlanId: str
    Title: str
    Description: str
    Channel: str
    Device: str
    CreatedAt: str
    FirstResponseAt: str
    ClosedAt: str
