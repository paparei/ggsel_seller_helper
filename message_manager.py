import asyncio
from datetime import datetime

class MessageManager:
    def __init__(self, db):
        self.db = db
        self._lock = asyncio.Lock()
    
    async def add_processed_message(self, chat_id: int, message_id: str, content: str, timestamp: datetime, sent_to_telegram: bool = False) -> bool:
        async with self._lock:
            # Check if processed
            with __import__('sqlite3').connect(self.db.db_path) as conn:
                cur = conn.execute("SELECT 1 FROM messages WHERE chat_id = ? AND message_id = ?", (chat_id, message_id))
                if cur.fetchone(): return False
                
                # We save using the main DB class
                from database import Message
                msg = Message(chat_id=chat_id, message_id=message_id, content=content, timestamp=timestamp, is_sent_to_telegram=sent_to_telegram)
                return self.db.save_message(msg)

    def is_message_processed(self, chat_id: int, message_id: str) -> bool:
        with __import__('sqlite3').connect(self.db.db_path) as conn:
            cur = conn.execute("SELECT 1 FROM messages WHERE chat_id = ? AND message_id = ?", (chat_id, message_id))
            return cur.fetchone() is not None

    def mark_message_sent(self, chat_id: int, message_id: str) -> None:
        self.db.mark_message_sent(message_id)