import json
import logging
from typing import Dict, Optional
from datetime import datetime
from dataclasses import dataclass

@dataclass
class Purchase:
    invoice_id: int
    item_id: int
    content_id: int
    cart_uid: str
    name: str
    amount: float
    currency_type: str
    invoice_state: int
    purchase_date: str
    date_pay: str
    buyer_email: str
    buyer_account: str
    buyer_phone: str
    buyer_ip: str
    payment_method: str
    processed_at: str

class PurchaseManager:
    def __init__(self, db):
        self.db = db

    def parse_purchase_response(self, response_data: Dict, invoice_id: int) -> Optional[Purchase]:
        try:
            if response_data.get('retval') == 0 and 'content' in response_data:
                content = response_data['content']
                buyer_info = content.get('buyer_info', {})
                return Purchase(
                    invoice_id=invoice_id, item_id=content.get('item_id', 0), content_id=content.get('content_id', 0),
                    cart_uid=content.get('cart_uid', ''), name=content.get('name', ''), amount=float(content.get('amount', 0)),
                    currency_type=content.get('currency_type', 'USD'), invoice_state=content.get('invoice_state', 0),
                    purchase_date=content.get('purchase_date', ''), date_pay=content.get('date_pay', ''),
                    buyer_email=buyer_info.get('email', ''), buyer_account=buyer_info.get('account', ''),
                    buyer_phone=buyer_info.get('phone', ''), buyer_ip=buyer_info.get('ip_address', ''),
                    payment_method=buyer_info.get('payment_method', ''), processed_at=datetime.now().isoformat()
                )
        except Exception as e:
            logging.error(f"Error parsing purchase {invoice_id}: {e}")
        return None
    
    def add_purchase(self, purchase: Purchase) -> bool:
        try:
            with __import__('sqlite3').connect(self.db.db_path) as conn:
                # Check if exists
                cur = conn.execute("SELECT 1 FROM purchases WHERE invoice_id = ?", (str(purchase.invoice_id),))
                if cur.fetchone(): return False
                
                # Insert
                data = purchase.__dict__
                conn.execute("INSERT INTO purchases (invoice_id, data) VALUES (?, ?)", (str(purchase.invoice_id), json.dumps(data)))
                return True
        except Exception as e:
            logging.error(f"Error saving purchase: {e}")
            return False
            
    def is_purchase_processed(self, invoice_id: int) -> bool:
        with __import__('sqlite3').connect(self.db.db_path) as conn:
            cur = conn.execute("SELECT 1 FROM purchases WHERE invoice_id = ?", (str(invoice_id),))
            return cur.fetchone() is not None