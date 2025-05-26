from enum import Enum


class Status(Enum):
  PENDING = 'pending'
  IN_PROGRESS = 'in_progress'
  PROCESSED = 'processed'
  FAILED = 'failed'
