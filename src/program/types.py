from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Event:
    emitted_by: str
    item_id: Optional[str] = None
    run_at: datetime = datetime.now()

    @property
    def log_message(self):
        return f"Item ID {self.item_id}" if self.item_id else "No ID"
