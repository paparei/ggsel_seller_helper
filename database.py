import sqlite3
import json
from datetime import datetime
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

@dataclass
class Chat:
    id_i: int
    email: Optional[str]
    product: int
    last_message: str
    cnt_msg: int
    cnt_new: int
    telegram_topic_id: Optional[int] = None

@dataclass
class Message:
    chat_id: int
    message_id: str
    content: str
    timestamp: datetime
    is_sent_to_telegram: bool = False

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize the unified database structure"""
        with sqlite3.connect(self.db_path) as conn:
            # Legacy Chats & Messages
            conn.execute('''
                CREATE TABLE IF NOT EXISTS chats (
                    id_i INTEGER PRIMARY KEY, email TEXT, product INTEGER,
                    last_message TEXT, cnt_msg INTEGER, cnt_new INTEGER,
                    telegram_topic_id INTEGER, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, message_id TEXT UNIQUE,
                    content TEXT, timestamp TIMESTAMP, is_sent_to_telegram BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (chat_id) REFERENCES chats (id_i)
                )
            ''')
            
            # Optimized Key-Value stores replacing JSON files
            conn.execute('CREATE TABLE IF NOT EXISTS topics (topic_key TEXT PRIMARY KEY, data TEXT)')
            conn.execute('CREATE TABLE IF NOT EXISTS purchases (invoice_id TEXT PRIMARY KEY, data TEXT)')
            conn.execute('CREATE TABLE IF NOT EXISTS reviews (review_id TEXT PRIMARY KEY, hash TEXT)')
            
            conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id)')

    # --- Core Message Methods ---
    def save_message(self, message: Message) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('INSERT INTO messages (chat_id, message_id, content, timestamp, is_sent_to_telegram) VALUES (?, ?, ?, ?, ?)',
                            (message.chat_id, message.message_id, message.content, message.timestamp, message.is_sent_to_telegram))
                return True
        except sqlite3.IntegrityError: return False

    def mark_message_sent(self, message_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('UPDATE messages SET is_sent_to_telegram = TRUE WHERE message_id = ?', (message_id,))