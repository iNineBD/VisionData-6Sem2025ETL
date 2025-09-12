from dataclasses import dataclass

@dataclass
class Attachment:
    AttachmentId: str
    TicketId: str
    FileName: str
    MimeType: str
    SizeBytes: str
    StoragePath: str
    UploadedAt: str
