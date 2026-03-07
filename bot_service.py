import asyncio
import json
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from telegram import InlineKeyboardButton
from config import Config
from database import Database, Chat, Message
from ggsel_api import GGSelAPI
from telegram_bot import TelegramBot
from topic_manager import TopicManager
from message_manager import MessageManager
from purchase_manager import PurchaseManager, Purchase
from locales import locales, _
from autoresponder import AutoResponder

# Глобальный пул потоков для параллельных HTTP запросов
_executor = ThreadPoolExecutor(max_workers=20)

class BotService:
    def __init__(self, config: Config):
        self.config = config
        self.database = Database(config.database_path)
        self.ggsel_api = GGSelAPI(config)
        self.telegram_bot = TelegramBot(config)
        # Используем абсолютный путь для topics.json
        topics_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "topics.json")
        self.topic_manager = TopicManager(topics_file)
        self.message_manager = MessageManager()
        self.purchase_manager = PurchaseManager()
        self.autoresponder = AutoResponder()
        self.running = False
        self.last_auth_time = None
        self.auth_interval = 15 * 60
        self.flood_control_until = None
        self.message_flood_control_until = None
        self.pending_messages = []
        self.pending_topics = []
        self.pending_topics_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pending_topics.json")
        self.pending_history_loads = []  # Очередь загрузки истории сообщений
        self.awaiting_input = {}  # {chat_id: {"type": "...", "data": ...}}
        self.processed_reviews = {}  # {review_id: hash} - обработанные отзывы с хэшем содержимого
        self.processed_reviews_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "processed_reviews.json")
        self.failed_topics = {}  # {invoice_id: timestamp} - неудачные попытки создания топиков
        self.api_error_until = None  # Кулдаун при ошибках API/сети
        self.chat_locks = {}  # Блокировки для каждого чата
        
        self._load_pending_topics()
        self._load_processed_reviews()
    
    def _load_processed_reviews(self):
        """Загрузка обработанных отзывов из файла"""
        try:
            if os.path.exists(self.processed_reviews_file):
                with open(self.processed_reviews_file, 'r') as f:
                    data = json.load(f)
                    # Поддержка старого формата (список) и нового (словарь)
                    if isinstance(data, list):
                        self.processed_reviews = {str(rid): "" for rid in data}
                    else:
                        self.processed_reviews = data
                    if self.processed_reviews:
                        logging.info(f"Загружено {len(self.processed_reviews)} обработанных отзывов")
        except Exception as e:
            logging.error(f"Ошибка загрузки отзывов: {e}")
    
    def _save_processed_reviews(self):
        """Сохранение обработанных отзывов в файл"""
        try:
            with open(self.processed_reviews_file, 'w') as f:
                json.dump(self.processed_reviews, f)
        except Exception as e:
            logging.error(f"Ошибка сохранения отзывов: {e}")
    
    def _load_pending_topics(self):
        """Загрузка отложенных топиков из файла"""
        try:
            if os.path.exists(self.pending_topics_file):
                with open(self.pending_topics_file, 'r') as f:
                    data = json.load(f)
                    for item in data:
                        # Восстанавливаем Purchase объект
                        purchase = Purchase(
                            invoice_id=item['invoice_id'],
                            item_id=item.get('item_id', 0),
                            content_id=item.get('content_id', 0),
                            cart_uid=item.get('cart_uid', ''),
                            name=item.get('name', ''),
                            amount=item.get('amount', 0),
                            currency_type=item.get('currency_type', ''),
                            invoice_state=item.get('invoice_state', 0),
                            purchase_date=item.get('purchase_date', ''),
                            date_pay=item.get('date_pay', ''),
                            buyer_email=item.get('buyer_email', ''),
                            buyer_account=item.get('buyer_account', ''),
                            buyer_phone=item.get('buyer_phone', ''),
                            buyer_ip=item.get('buyer_ip', ''),
                            payment_method=item.get('payment_method', ''),
                            processed_at=item.get('processed_at', '')
                        )
                        self.pending_topics.append({
                            'purchase': purchase, 
                            'timestamp': datetime.now(),
                            'skip_greeting': item.get('skip_greeting', False)
                        })
                    if self.pending_topics:
                        logging.info(f"Загружено {len(self.pending_topics)} отложенных топиков")
        except Exception as e:
            logging.error(f"Ошибка загрузки отложенных топиков: {e}")
    
    def _save_pending_topics(self):
        """Сохранение отложенных топиков в файл"""
        try:
            data = []
            for item in self.pending_topics:
                p = item['purchase']
                data.append({
                    'invoice_id': p.invoice_id,
                    'item_id': p.item_id,
                    'content_id': p.content_id,
                    'cart_uid': p.cart_uid,
                    'name': p.name,
                    'amount': p.amount,
                    'currency_type': p.currency_type,
                    'invoice_state': p.invoice_state,
                    'purchase_date': p.purchase_date,
                    'date_pay': p.date_pay,
                    'buyer_email': p.buyer_email,
                    'buyer_account': p.buyer_account,
                    'buyer_phone': p.buyer_phone,
                    'buyer_ip': p.buyer_ip,
                    'payment_method': p.payment_method,
                    'processed_at': p.processed_at,
                    'skip_greeting': item.get('skip_greeting', False)
                })
            with open(self.pending_topics_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logging.error(f"Ошибка сохранения отложенных топиков: {e}")
        
    async def start(self):
        """Запуск бота"""
        logging.info("Запуск GGSel бота...")
        
        self.telegram_bot.set_topic_message_handler(self.handle_topic_message)
        self.telegram_bot.set_callback_handler(self.handle_callback)
        self.telegram_bot.set_general_message_handler(self.handle_general_message)
        self.telegram_bot.set_history_handler(self.handle_history_command)
        self.telegram_bot.set_options_handler(self.handle_options_command)
        self.telegram_bot.set_review_handler(self.handle_review_command)
        
        await self.telegram_bot.start()
        
        if not await self.ensure_ggsel_auth():
            logging.error("Ошибка авторизации GGSel API")
            return
        
        # Проверка API отзывов при запуске
        await self.test_reviews_api()
        
        await self.process_pending_topics()
        
        logging.info("Бот запущен")
        self.running = True
        
        tasks = [
            asyncio.create_task(self.monitor_messages()),
            asyncio.create_task(self.reauth_scheduler()),
            asyncio.create_task(self.purchase_checker())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await self.stop()
    
    async def test_reviews_api(self):
        """Тест API отзывов при запуске"""
        try:
            loop = asyncio.get_event_loop()
            reviews_data = await loop.run_in_executor(
                _executor, lambda: self.ggsel_api.get_reviews(5)
            )
            
            if not reviews_data:
                logging.warning("API отзывов: нет данных")
                return
            
            reviews = reviews_data.get('reviews', [])
            logging.info(f"API отзывов: получено {len(reviews)} отзывов")
            
            if reviews:
                r = reviews[0]
                # Показываем все ключи для диагностики
                logging.info(f"Структура отзыва: {list(r.keys())}")
                logging.info(f"Пример: id={r.get('id')}, invoice_id={r.get('invoice_id')}, good={r.get('good')}, type={r.get('type')}")
        except Exception as e:
            logging.error(f"Ошибка теста API отзывов: {e}")
    
    def handle_topic_message(self, topic_id: int, message_text: str, username: str, message_id: int):
        """Обработка сообщения в топике"""
        asyncio.create_task(self._handle_topic_message_async(topic_id, message_text, username, message_id))
    
    async def handle_general_message(self, text: str):
        """Обработка сообщений в General (для настроек автоответов)"""
        chat_id = self.config.telegram_group_id
        if chat_id in self.awaiting_input:
            await self.handle_text_input(chat_id, text)
    
    async def _handle_topic_message_async(self, topic_id: int, message_text: str, username: str, message_id: int):
        """Асинхронная отправка сообщения из ТГ в GGSel"""
        try:
            # Не отправляем команды (начинаются с /)
            if message_text.startswith('/'):
                return
            
            all_topics = self.topic_manager.get_all_topics()
            
            target_topic = None
            target_key = None
            for key, topic_info in all_topics.items():
                if topic_info.get('topic_id') == topic_id:
                    target_topic = topic_info
                    target_key = key
                    break
            
            if not target_topic:
                return
            
            # Получаем invoice_id топика — это и есть id_i чата
            invoice_id = target_topic.get('invoice_id')
            if not invoice_id:
                await self.send_message_with_cooldown("⚠️ Нет invoice_id", topic_id)
                return
            
            # Отправляем только в чат с id_i == invoice_id
            loop = asyncio.get_event_loop()
            try:
                result = await loop.run_in_executor(
                    None, 
                    lambda: self.ggsel_api.send_message(invoice_id, message_text)
                )
                if result:
                    await self.telegram_bot.add_reaction(message_id, topic_id, "🔥")
                else:
                    await self.send_message_with_cooldown("❌ Ошибка отправки", topic_id)
            except Exception as e:
                logging.error(f"Ошибка отправки в чат {invoice_id}: {e}")
                await self.send_message_with_cooldown("❌ Ошибка отправки", topic_id)
            
        except Exception as e:
            logging.error(f"Ошибка обработки сообщения: {e}")
    
    async def ensure_ggsel_auth(self) -> bool:
        """Авторизация в GGSel API"""
        current_time = datetime.now()
        
        if self.last_auth_time and current_time - self.last_auth_time < timedelta(seconds=self.auth_interval):
            return True
        
        loop = asyncio.get_event_loop()
        success = await loop.run_in_executor(None, self.ggsel_api.login)
        
        if success:
            self.last_auth_time = current_time
            return True
        return False
    
    async def purchase_checker(self):
        """Проверка покупок каждые 30 секунд"""
        while self.running:
            try:
                await self.check_new_purchases()
            except Exception as e:
                logging.error(f"Ошибка проверки покупок: {e}")
            await asyncio.sleep(30)
    
    async def check_new_purchases(self):
        """Проверка новых покупок"""
        if not await self.ensure_ggsel_auth():
            return
        
        loop = asyncio.get_event_loop()
        try:
            sales_data = await loop.run_in_executor(None, self.ggsel_api.get_last_sales, 10)
        except Exception as e:
            logging.debug(f"Ошибка получения продаж: {e}")
            return
        
        if not sales_data or sales_data.get('retval') != 0:
            return
        
        for sale in sales_data.get('sales', []):
            invoice_id = sale.get('invoice_id')
            if not invoice_id:
                continue
            
            # Пропускаем если недавно была неудачная попытка (10 минут)
            if invoice_id in self.failed_topics:
                failed_time = self.failed_topics[invoice_id]
                if datetime.now() - failed_time < timedelta(minutes=10):
                    continue
                del self.failed_topics[invoice_id]
            
            if not self.purchase_manager.is_purchase_processed(invoice_id):
                await self.process_new_purchase(invoice_id)
    
    async def process_new_purchase(self, invoice_id: int):
        """Обработка новой покупки"""
        try:
            # Проверяем не было ли недавней неудачной попытки
            failed_time = self.failed_topics.get(invoice_id)
            if failed_time and datetime.now() - failed_time < timedelta(minutes=5):
                return
            
            if not await self.ensure_ggsel_auth():
                # Ошибка авторизации - запоминаем и пропускаем
                self.failed_topics[invoice_id] = datetime.now()
                return
            
            loop = asyncio.get_event_loop()
            purchase_data = await loop.run_in_executor(
                None, self.ggsel_api.get_purchase_info, invoice_id
            )
            
            if not purchase_data:
                # Помечаем как обработанную чтобы не спамить при ошибках сети
                dummy = type('Purchase', (), {
                    'invoice_id': invoice_id, 'buyer_email': '', 'buyer_account': '',
                    'name': 'Unknown', 'amount': 0, 'currency_type': 'USD',
                    'purchase_date': '', 'date_pay': '', 'buyer_phone': '',
                    'buyer_ip': '', 'payment_method': '', 'processed_at': datetime.now().isoformat()
                })()
                self.purchase_manager.add_purchase(dummy)
                logging.warning(f"Не удалось получить инфо о покупке {invoice_id}, пропускаем")
                return
            
            purchase = self.purchase_manager.parse_purchase_response(purchase_data, invoice_id)
            if purchase and self.purchase_manager.add_purchase(purchase):
                logging.info(f"Покупка: {purchase.invoice_id} - {purchase.buyer_email}")
                await self.create_topic_for_purchase(purchase)
                
        except Exception as e:
            self.failed_topics[invoice_id] = datetime.now()
            logging.error(f"Ошибка обработки покупки {invoice_id}: {e}")
    
    async def create_topic_for_purchase(self, purchase: Purchase, skip_greeting: bool = False):
        """Создание топика для покупки"""
        try:
            # Проверяем не было ли недавней неудачной попытки (5 минут кулдаун)
            failed_time = self.failed_topics.get(purchase.invoice_id)
            if failed_time and datetime.now() - failed_time < timedelta(minutes=5):
                return  # Тихо пропускаем
            
            if self.flood_control_until and datetime.now() < self.flood_control_until:
                logging.info(f"Flood control, добавляем в очередь: {purchase.invoice_id}")
                self.pending_topics.append({'purchase': purchase, 'timestamp': datetime.now(), 'skip_greeting': skip_greeting})
                self._save_pending_topics()
                return
            self.flood_control_until = None
            
            # Проверяем по invoice_id
            topic_key = f"purchase_{purchase.invoice_id}"
            if self.topic_manager.get_all_topics().get(topic_key):
                return
            
            customer_id = purchase.buyer_email or purchase.buyer_account or f"Customer_{purchase.invoice_id}"
            
            # Название: ID | email
            topic_name = f"💬 {purchase.invoice_id} | {customer_id}"
            
            # Задержка перед созданием топика (антифлуд)
            await asyncio.sleep(2)
            
            topic_id, cooldown = await self.telegram_bot.create_topic(topic_name)
            
            if topic_id is not None:
                # Используем invoice_id как id_i чата (без поиска по email)
                self.topic_manager.add_topic_for_purchase(purchase, topic_id, topic_name)
                
                # Форматирование даты
                date_str = ""
                if purchase.purchase_date:
                    try:
                        dt = datetime.fromisoformat(purchase.purchase_date.replace('+03:00', ''))
                        date_str = dt.strftime('%d.%m.%Y %H:%M')
                    except:
                        date_str = purchase.purchase_date
                
                msg = f"🛒 {'Восстановлен топик' if skip_greeting else 'Новая покупка'}\n\n"
                msg += f"🧾 Invoice: {purchase.invoice_id}\n"
                msg += f"📦 {purchase.name}\n"
                msg += f"💰 {purchase.amount} {purchase.currency_type}\n"
                msg += f"📧 {purchase.buyer_email or 'N/A'}\n"
                if purchase.buyer_account:
                    msg += f"👤 {purchase.buyer_account}\n"
                if purchase.payment_method:
                    msg += f"💳 {purchase.payment_method}\n"
                if date_str:
                    msg += f"📅 {date_str}\n"
                
                # Получаем опции покупки
                options_text, options_list = await self.get_purchase_options_with_list(purchase.invoice_id)
                if options_text:
                    msg += f"\n⚙️ Опции:\n{options_text}\n"
                
                await self.send_message_with_cooldown(msg, topic_id)
                logging.info(f"Создан топик {topic_id} для {purchase.invoice_id}")
                
                # Проверяем режим ЧСВ
                if options_list and not skip_greeting:
                    await self.process_csv_rules(purchase.invoice_id, topic_id, options_list)
                
                # Отправляем приветствие в чат покупки (invoice_id = id_i чата)
                if not skip_greeting and self.autoresponder.should_send_first_message():
                    greeting = self.autoresponder.get_first_message_text()
                    if greeting:
                        loop = asyncio.get_event_loop()
                        try:
                            await loop.run_in_executor(
                                None, 
                                lambda cid=purchase.invoice_id, g=greeting: self.ggsel_api.send_message(cid, g)
                            )
                            await self.send_message_with_cooldown(f"📤 {greeting}", topic_id)
                            logging.info(f"Приветствие отправлено в чат {purchase.invoice_id}")
                        except Exception as e:
                            logging.error(f"Ошибка отправки приветствия: {e}")
                    
            elif cooldown:
                self.flood_control_until = datetime.now() + timedelta(seconds=cooldown + 5)
                self.pending_topics.append({'purchase': purchase, 'timestamp': datetime.now(), 'skip_greeting': skip_greeting})
                self._save_pending_topics()
                logging.warning(f"Flood control {cooldown}s, в очередь: {purchase.invoice_id}")
            else:
                self.failed_topics[purchase.invoice_id] = datetime.now()
                logging.error(f"Не удалось создать топик для {purchase.invoice_id}, повтор через 5 мин")
                
        except Exception as e:
            self.failed_topics[purchase.invoice_id] = datetime.now()
            logging.error(f"Ошибка создания топика покупки {purchase.invoice_id}: {e}")
    
    async def load_chat_history(self, chat_ids: List[int], topic_id: int, force_reload: bool = False):
        """Загрузка истории сообщений из GGSel и отправка в топик"""
        try:
            all_messages = []
            loop = asyncio.get_event_loop()
            
            # Собираем сообщения из всех чатов
            for chat_id in chat_ids:
                messages_data = await loop.run_in_executor(
                    None, self.ggsel_api.get_chat_messages, chat_id
                )
                
                if messages_data:
                    for msg in messages_data:
                        msg['_chat_id'] = chat_id
                        all_messages.append(msg)
            
            # Если история пустая - отправляем приветствие
            if not all_messages and self.autoresponder.should_send_first_message():
                greeting = self.autoresponder.get_first_message_text()
                if greeting and chat_ids:
                    # Отправляем в GGSel
                    for chat_id in chat_ids:
                        await loop.run_in_executor(
                            None, 
                            lambda cid=chat_id, g=greeting: self.ggsel_api.send_message(cid, g)
                        )
                    # И в топик (без пометки)
                    await self.send_message_with_cooldown(greeting, topic_id)
                return
            
            if not all_messages:
                return
            
            # Сортируем по времени (от старых к новым)
            def get_timestamp(msg):
                ts = msg.get('timestamp', msg.get('created_at', msg.get('date', msg.get('time', ''))))
                if not ts:
                    return datetime.min
                try:
                    # Пробуем разные форматы
                    ts = str(ts).replace('Z', '+00:00').replace('+03:00', '')
                    return datetime.fromisoformat(ts)
                except:
                    try:
                        return datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                    except:
                        return datetime.min
            
            all_messages.sort(key=get_timestamp)
            
            logging.info(f"Загружено {len(all_messages)} сообщений для топика {topic_id}")
            
            # Отправляем сообщения в топик
            for msg in all_messages:
                message_id = str(msg.get('id', ''))
                content = msg.get('message', msg.get('text', msg.get('content', '')))
                chat_id = msg.get('_chat_id')
                timestamp = get_timestamp(msg)
                
                if not content:
                    continue
                
                # Проверяем, не обработано ли уже (кроме случая force_reload)
                if not force_reload and self.message_manager.is_message_processed(chat_id, message_id):
                    continue
                
                # Просто текст сообщения с пометкой истории при force_reload
                message_text = f"📜 {content}" if force_reload else content
                await self.send_message_with_cooldown(message_text, topic_id, chat_id, message_id)
                
                # Помечаем как обработанное (только если не force_reload)
                if not force_reload:
                    await self.message_manager.add_processed_message(chat_id, message_id, content, timestamp)
                
                await asyncio.sleep(0.5)
                
        except Exception as e:
            logging.error(f"Ошибка загрузки истории: {e}")
    
    async def process_pending_topics(self):
        """Обработка отложенных топиков"""
        if not self.pending_topics:
            # Но всё равно проверяем очередь истории
            await self.process_pending_history_loads()
            return
        
        if self.flood_control_until and datetime.now() < self.flood_control_until:
            remaining = (self.flood_control_until - datetime.now()).seconds
            logging.debug(f"Flood control активен, ждём {remaining}s, в очереди {len(self.pending_topics)} топиков")
            return
        self.flood_control_until = None
        
        topics = self.pending_topics.copy()
        self.pending_topics.clear()
        self._save_pending_topics()  # Очищаем файл
        
        logging.info(f"Обрабатываем {len(topics)} отложенных топиков")
        
        for i, data in enumerate(topics):
            # Пропускаем топики добавленные менее 30 секунд назад (защита от спама при ошибках)
            timestamp = data.get('timestamp')
            if timestamp and (datetime.now() - timestamp).total_seconds() < 30:
                self.pending_topics.append(data)
                continue
            
            skip_greeting = data.get('skip_greeting', False)
            await self.create_topic_for_purchase(data['purchase'], skip_greeting=skip_greeting)
            if self.flood_control_until:
                # Возвращаем необработанные топики в очередь
                remaining_topics = topics[i+1:]
                for t in remaining_topics:
                    if not any(p['purchase'].invoice_id == t['purchase'].invoice_id for p in self.pending_topics):
                        self.pending_topics.append(t)
                self._save_pending_topics()
                logging.info(f"Flood control, {len(remaining_topics)} топиков вернулись в очередь")
                break
            await asyncio.sleep(3)  # Увеличенная задержка
        
        # Загружаем историю для созданных топиков
        await self.process_pending_history_loads()
    
    async def monitor_messages(self):
        """Мониторинг сообщений - проверяет все топики каждый цикл"""
        logging.info("Запуск мониторинга сообщений")
        sync_counter = 0
        review_counter = 0
        
        # Синхронизация в фоне, не блокируем основной цикл
        asyncio.create_task(self.sync_topics_with_purchases())
        
        # Проверяем отзывы сразу при запуске
        asyncio.create_task(self.check_new_reviews())
        
        while self.running:
            try:
                await self.process_pending_messages()
                await self.process_pending_topics()
                
                if not await self.ensure_ggsel_auth():
                    await asyncio.sleep(2)
                    continue
                
                # Получаем все топики покупок и проверяем их
                all_topics = self.topic_manager.get_all_topics()
                purchase_topics = {k: v for k, v in all_topics.items() if k.startswith('purchase_')}
                
                if purchase_topics:
                    await self.check_topics_parallel(purchase_topics)
                
                # Проверяем отзывы каждые 3 цикла (~6 секунд)
                review_counter += 1
                if review_counter >= 3:
                    review_counter = 0
                    asyncio.create_task(self.check_new_reviews())
                
                # Синхронизация топиков раз в день (43200 циклов * 2 сек = 24 часа)
                sync_counter += 1
                if sync_counter >= 43200:
                    sync_counter = 0
                    asyncio.create_task(self.sync_topics_with_purchases())
                
            except Exception as e:
                logging.error(f"Ошибка мониторинга: {e}")
            
            await asyncio.sleep(2)  # 2 секунды между циклами
    
    async def check_topics_parallel(self, topics: Dict):
        """Параллельная проверка топиков с семафором"""
        if not topics:
            return
        
        # Семафор для ограничения параллельных запросов (10 одновременно)
        semaphore = asyncio.Semaphore(10)
        
        async def check_with_semaphore(invoice_id: int, topic_id: int):
            async with semaphore:
                await self._check_single_chat(invoice_id, topic_id)
        
        tasks = []
        for topic_key, topic_info in topics.items():
            topic_id = topic_info.get('topic_id')
            invoice_id = topic_info.get('invoice_id')
            
            if topic_id and invoice_id:
                tasks.append(check_with_semaphore(invoice_id, topic_id))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _check_single_chat(self, chat_id: int, topic_id: int):
        """Проверка одного чата с блокировкой от дублирования"""
        # Получаем или создаем блокировку для этого чата
        if chat_id not in self.chat_locks:
            self.chat_locks[chat_id] = asyncio.Lock()
        
        async with self.chat_locks[chat_id]:
            try:
                await self.check_chat_messages(chat_id, topic_id)
            except Exception as e:
                logging.error(f"Ошибка проверки чата {chat_id}: {e}")
    
    async def check_chat_messages(self, chat_id: int, topic_id: int) -> bool:
        """Проверка сообщений чата. Возвращает True если было новое сообщение"""
        try:
            loop = asyncio.get_event_loop()
            messages_data = await loop.run_in_executor(
                _executor, self.ggsel_api.get_chat_messages, chat_id
            )
            
            if not messages_data:
                return False
            
            has_new = False
            for msg_data in messages_data:
                if await self.process_single_message_check(chat_id, topic_id, msg_data):
                    has_new = True
            
            return has_new
                
        except Exception as e:
            logging.error(f"Ошибка проверки чата {chat_id}: {e}")
            return False
    
    async def process_single_message_check(self, chat_id: int, topic_id: int, msg_data: Dict) -> bool:
        """Обработка сообщения"""
        try:
            message_id = str(msg_data.get('id', ''))
            content = msg_data.get('message', msg_data.get('text', msg_data.get('content', '')))
            timestamp_str = msg_data.get('timestamp', msg_data.get('created_at', ''))
            
            if not message_id or not content:
                return False
            
            # Проверяем по уникальному ключу chat_id + message_id
            if self.message_manager.is_message_processed(chat_id, message_id):
                return False
            
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00').replace('+03:00', '')) if timestamp_str else datetime.now()
            except:
                timestamp = datetime.now()
            
            # Сохраняем с правильным chat_id для уникальности
            if await self.message_manager.add_processed_message(chat_id, message_id, content, timestamp):
                message = Message(chat_id=chat_id, message_id=message_id, content=content, timestamp=timestamp)
                self.database.save_message(message)
                
                logging.info(f"Новое сообщение в чате {chat_id}: {content[:50]}...")
                
                # Просто текст
                await self.send_message_with_cooldown(content, topic_id, chat_id, message_id)
                
                # Проверяем автоответ на триггеры
                try:
                    auto_result = self.autoresponder.find_response(content)
                    if auto_result:
                        response_text = auto_result.get("response", "")
                        notify_group = auto_result.get("notify_group", False)
                        
                        if response_text:
                            # Отправляем автоответ в GGSel
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(
                                None, 
                                lambda rt=response_text: self.ggsel_api.send_message(chat_id, rt)
                            )
                            # И в топик
                            await self.send_message_with_cooldown(response_text, topic_id)
                        
                        # Уведомление в тот же топик
                        if notify_group:
                            topic_info = None
                            for key, info in self.topic_manager.get_all_topics().items():
                                if info.get('topic_id') == topic_id:
                                    topic_info = info
                                    break
                            
                            trigger_notify_text = auto_result.get("notify_text", "")
                            notify_msg = trigger_notify_text or "🔔 Требуется ответ!"
                            if topic_info:
                                notify_msg += f"\n📧 {topic_info.get('email', 'N/A')}"
                                notify_msg += f"\n🆔 {topic_info.get('invoice_id', 'N/A')}"
                            
                            await self.send_message_with_cooldown(notify_msg, topic_id)
                except Exception as e:
                    logging.error(f"Ошибка автоответа: {e}")
                
                return True
            
            return False
                
        except Exception as e:
            logging.error(f"Ошибка обработки сообщения: {e}")
            return False
    
    async def stop(self):
        """Остановка бота"""
        logging.info("Остановка бота...")
        self.running = False
        
        tasks = [t for t in asyncio.all_tasks() if not t.done()]
        for task in tasks:
            task.cancel()
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        await self.telegram_bot.stop()
        logging.info("Бот остановлен")
    
    async def send_message_with_cooldown(self, text: str, topic_id: int, chat_id: int = None, message_id: str = None) -> bool:
        """Отправка с учетом кулдауна и проверкой дублей"""
        try:
            # Проверяем, не было ли это сообщение уже отправлено в Telegram
            if chat_id and message_id:
                key = f"{chat_id}_{message_id}"
                if key in self.message_manager.processed_messages:
                    msg_data = self.message_manager.processed_messages[key]
                    if msg_data.get("sent_to_telegram", False):
                        logging.debug(f"Сообщение {message_id} уже отправлено в Telegram, пропускаем")
                        return True
            
            if self.message_flood_control_until and datetime.now() < self.message_flood_control_until:
                self.pending_messages.append({
                    'text': text, 'topic_id': topic_id,
                    'chat_id': chat_id, 'message_id': message_id,
                    'timestamp': datetime.now()
                })
                return False
            self.message_flood_control_until = None
            
            success, cooldown = await self.telegram_bot.send_message(text, topic_id)
            
            if success:
                if chat_id and message_id:
                    self.message_manager.mark_message_sent(chat_id, message_id)
                    self.database.mark_message_sent(message_id)
                return True
                
            elif cooldown:
                self.message_flood_control_until = datetime.now() + timedelta(seconds=cooldown + 5)
                self.pending_messages.append({
                    'text': text, 'topic_id': topic_id,
                    'chat_id': chat_id, 'message_id': message_id,
                    'timestamp': datetime.now()
                })
            return False
                
        except Exception as e:
            logging.error(f"Ошибка отправки: {e}")
            return False
    
    async def process_pending_messages(self):
        """Обработка отложенных сообщений"""
        if not self.pending_messages:
            return
        
        if self.message_flood_control_until and datetime.now() < self.message_flood_control_until:
            return
        self.message_flood_control_until = None
        
        messages = self.pending_messages.copy()
        self.pending_messages.clear()
        
        for msg in messages:
            success = await self.send_message_with_cooldown(
                msg['text'], msg['topic_id'], msg.get('chat_id'), msg.get('message_id')
            )
            if not success and self.message_flood_control_until:
                break
            await asyncio.sleep(1)
    
    async def reauth_scheduler(self):
        """Переавторизация каждые 15 минут"""
        while self.running:
            await asyncio.sleep(self.auth_interval)
            if self.running:
                await self.ensure_ggsel_auth()
    
    def stop_sync(self):
        """Синхронная остановка"""
        self.running = False

    # ==================== АВТООТВЕТЫ И INLINE КНОПКИ ====================
    
    def _safe_parse_idx(self, data: str, prefix: str) -> int:
        """Безопасный парсинг индекса из callback data"""
        try:
            return int(data.replace(prefix, ""))
        except (ValueError, TypeError):
            return -1
    
    async def handle_callback(self, data: str, update, context):
        """Обработка inline кнопок"""
        query = update.callback_query
        
        if data == "auto_menu_new":
            chat_id = update.effective_chat.id
            await self.send_auto_menu_new(chat_id)
            return
        
        message = query.message
        chat_id = message.chat.id
        message_id = message.message_id
        
        if data == "auto_menu":
            await self.show_auto_menu(chat_id, message_id)
        
        elif data == "auto_toggle":
            self.autoresponder.toggle_enabled()
            await self.show_auto_menu(chat_id, message_id)
        
        elif data == "auto_first_toggle":
            self.autoresponder.toggle_first_message()
            await self.show_auto_menu(chat_id, message_id)
        
        elif data == "auto_first_edit":
            self.awaiting_input[chat_id] = {"type": "first_message"}
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                _("prompt_greeting"),
                [[InlineKeyboardButton(_("btn_cancel"), callback_data="auto_menu")]]
            )
        
        elif data == "auto_triggers":
            await self.show_triggers_menu(chat_id, message_id)
        
        elif data == "auto_add_trigger":
            self.awaiting_input[chat_id] = {"type": "trigger_phrase"}
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                _("prompt_trigger_phrase"),
                [[InlineKeyboardButton(_("btn_cancel"), callback_data="auto_triggers")]]
            )
        
        elif data == "auto_add_trigger_notify":
            self.awaiting_input[chat_id] = {"type": "trigger_phrase", "notify_group": True}
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                _("prompt_trigger_notify"),
                [[InlineKeyboardButton(_("btn_cancel"), callback_data="auto_triggers")]]
            )
        
        elif data.startswith("auto_trigger_edit_"):
            idx = self._safe_parse_idx(data, "auto_trigger_edit_")
            if idx >= 0:
                await self.show_trigger_edit_menu(chat_id, message_id, idx)
        
        elif data.startswith("auto_trigger_notify_"):
            idx = self._safe_parse_idx(data, "auto_trigger_notify_")
            if idx >= 0:
                self.autoresponder.toggle_trigger_notify(idx)
                await self.show_trigger_edit_menu(chat_id, message_id, idx)
        
        elif data.startswith("auto_trigger_exact_"):
            idx = self._safe_parse_idx(data, "auto_trigger_exact_")
            if idx >= 0:
                self.autoresponder.toggle_trigger_exact_match(idx)
                await self.show_trigger_edit_menu(chat_id, message_id, idx)
        
        elif data.startswith("auto_trigger_toggle_"):
            idx = self._safe_parse_idx(data, "auto_trigger_toggle_")
            if idx >= 0:
                self.autoresponder.toggle_trigger(idx)
                await self.show_trigger_edit_menu(chat_id, message_id, idx)
        
        elif data.startswith("auto_trigger_phrase_"):
            idx = self._safe_parse_idx(data, "auto_trigger_phrase_")
            if idx >= 0:
                self.awaiting_input[chat_id] = {"type": "edit_trigger_phrase", "trigger_idx": idx}
                await self.telegram_bot.edit_message(
                    message_id, chat_id,
                    _("prompt_edit_phrase"),
                    [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"auto_trigger_edit_{idx}")]]
                )
        
        elif data.startswith("auto_trigger_response_"):
            idx = self._safe_parse_idx(data, "auto_trigger_response_")
            if idx >= 0:
                self.awaiting_input[chat_id] = {"type": "edit_trigger_response", "trigger_idx": idx}
                await self.telegram_bot.edit_message(
                    message_id, chat_id,
                    _("prompt_edit_answer"),
                    [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"auto_trigger_edit_{idx}")]]
                )
        
        elif data.startswith("auto_trigger_notifytext_"):
            idx = self._safe_parse_idx(data, "auto_trigger_notifytext_")
            if idx >= 0:
                self.awaiting_input[chat_id] = {"type": "edit_trigger_notify_text", "trigger_idx": idx}
                trigger = self.autoresponder.get_trigger(idx)
                current = trigger.get('notify_text', '') if trigger else ''
                text_prompt = _("prompt_notify_text").replace("{current}", current or "(не задан)")
                await self.telegram_bot.edit_message(
                    message_id, chat_id,
                    text_prompt,
                    [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"auto_trigger_edit_{idx}")]]
                )
        
        elif data.startswith("auto_trigger_del_"):
            idx = self._safe_parse_idx(data, "auto_trigger_del_")
            if idx >= 0:
                self.autoresponder.remove_trigger(idx)
                await self.show_triggers_menu(chat_id, message_id)
        
        elif data == "auto_reviews":
            await self.show_reviews_menu(chat_id, message_id)
        
        elif data == "auto_reviews_toggle":
            self.autoresponder.toggle_review_responses()
            await self.show_reviews_menu(chat_id, message_id)
        
        elif data == "auto_reviews_good_toggle":
            self.autoresponder.toggle_good_review_response()
            await self.show_reviews_menu(chat_id, message_id)
        
        elif data == "auto_reviews_bad_toggle":
            self.autoresponder.toggle_bad_review_response()
            await self.show_reviews_menu(chat_id, message_id)
        
        elif data == "auto_reviews_good_edit":
            self.awaiting_input[chat_id] = {"type": "edit_good_review_text"}
            current = self.autoresponder.get_good_review_text()
            text_prompt = _("prompt_good_review").replace("{current}", current)
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                text_prompt,
                [[InlineKeyboardButton(_("btn_cancel"), callback_data="auto_reviews")]]
            )
        
        elif data == "auto_reviews_bad_edit":
            self.awaiting_input[chat_id] = {"type": "edit_bad_review_text"}
            current = self.autoresponder.get_bad_review_text()
            text_prompt = _("prompt_bad_review").replace("{current}", current)
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                text_prompt,
                [[InlineKeyboardButton(_("btn_cancel"), callback_data="auto_reviews")]]
            )
        
        elif data == "stats":
            topics = self.topic_manager.get_all_topics()
            purchases = len([k for k in topics if k.startswith('purchase_')])
            text = f"{_('stats_title')}\n\n"
            text += f"{_('stats_topics')} {len(topics)}\n"
            text += f"{_('stats_purchases')} {purchases}\n"
            text += f"{_('stats_msgs')} {len(self.message_manager.processed_messages)}\n"
            text += f"{_('stats_auto')} {'✅' if self.autoresponder.is_enabled() else '❌'}"
            keyboard = [[InlineKeyboardButton(_("btn_back"), callback_data="auto_menu")]]
            await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
        
        elif data == "csv_menu":
            await self.show_csv_menu(chat_id, message_id)
        
        elif data == "csv_toggle":
            self.autoresponder.toggle_csv_mode()
            await self.show_csv_menu(chat_id, message_id)
        
        elif data == "csv_add_rule":
            self.awaiting_input[chat_id] = {"type": "csv_option_name"}
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                _("prompt_csv_option"),
                [[InlineKeyboardButton(_("btn_cancel"), callback_data="csv_menu")]]
            )
        
        elif data.startswith("csv_rule_"):
            idx = self._safe_parse_idx(data, "csv_rule_")
            if idx >= 0:
                await self.show_csv_rule_menu(chat_id, message_id, idx)
        
        elif data.startswith("csv_toggle_"):
            idx = self._safe_parse_idx(data, "csv_toggle_")
            if idx >= 0:
                self.autoresponder.toggle_csv_rule(idx)
                await self.show_csv_rule_menu(chat_id, message_id, idx)
        
        elif data.startswith("csv_case_"):
            idx = self._safe_parse_idx(data, "csv_case_")
            if idx >= 0:
                self.autoresponder.toggle_csv_rule_case_sensitive(idx)
                await self.show_csv_rule_menu(chat_id, message_id, idx)
        
        elif data.startswith("csv_matchtype_"):
            idx = self._safe_parse_idx(data, "csv_matchtype_")
            if idx >= 0:
                self.autoresponder.cycle_csv_rule_match_type(idx)
                await self.show_csv_rule_menu(chat_id, message_id, idx)
        
        elif data.startswith("csv_value_"):
            idx = self._safe_parse_idx(data, "csv_value_")
            if idx >= 0:
                self.awaiting_input[chat_id] = {"type": "csv_option_value", "rule_idx": idx}
                rule = self.autoresponder.get_csv_rule(idx)
                current = rule.get("option_value", "") if rule else ""
                text_prompt = _("prompt_csv_value").replace("{current}", current or "(любое)")
                await self.telegram_bot.edit_message(
                    message_id, chat_id,
                    text_prompt,
                    [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"csv_rule_{idx}")]]
                )
        
        elif data.startswith("csv_touser_"):
            idx = self._safe_parse_idx(data, "csv_touser_")
            if idx >= 0:
                self.autoresponder.toggle_csv_rule_send_to_user(idx)
                await self.show_csv_rule_menu(chat_id, message_id, idx)
        
        elif data.startswith("csv_totopic_"):
            idx = self._safe_parse_idx(data, "csv_totopic_")
            if idx >= 0:
                self.autoresponder.toggle_csv_rule_send_to_topic(idx)
                await self.show_csv_rule_menu(chat_id, message_id, idx)
        
        elif data.startswith("csv_usermsg_"):
            idx = self._safe_parse_idx(data, "csv_usermsg_")
            if idx >= 0:
                self.awaiting_input[chat_id] = {"type": "csv_user_message", "rule_idx": idx}
                rule = self.autoresponder.get_csv_rule(idx)
                current = rule.get("user_message", "") if rule else ""
                text_prompt = _("prompt_csv_user").replace("{current}", current or "(не задано)")
                await self.telegram_bot.edit_message(
                    message_id, chat_id,
                    text_prompt,
                    [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"csv_rule_{idx}")]]
                )
        
        elif data.startswith("csv_topicmsg_"):
            idx = self._safe_parse_idx(data, "csv_topicmsg_")
            if idx >= 0:
                self.awaiting_input[chat_id] = {"type": "csv_topic_message", "rule_idx": idx}
                rule = self.autoresponder.get_csv_rule(idx)
                current = rule.get("topic_message", "") if rule else ""
                text_prompt = _("prompt_csv_topic").replace("{current}", current or "(не задано)")
                await self.telegram_bot.edit_message(
                    message_id, chat_id,
                    text_prompt,
                    [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"csv_rule_{idx}")]]
                )
        
        elif data.startswith("csv_name_"):
            idx = self._safe_parse_idx(data, "csv_name_")
            if idx >= 0:
                self.awaiting_input[chat_id] = {"type": "csv_edit_name", "rule_idx": idx}
                rule = self.autoresponder.get_csv_rule(idx)
                current = rule.get("option_name", "") if rule else ""
                text_prompt = _("prompt_csv_name").replace("{current}", current)
                await self.telegram_bot.edit_message(
                    message_id, chat_id,
                    text_prompt,
                    [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"csv_rule_{idx}")]]
                )
        
        elif data.startswith("csv_del_"):
            idx = self._safe_parse_idx(data, "csv_del_")
            if idx >= 0:
                self.autoresponder.remove_csv_rule(idx)
                await self.show_csv_menu(chat_id, message_id)
    
    async def show_auto_menu(self, chat_id: int, message_id: int):
        """Показать меню автоответов (Translated)"""
        enabled = self.autoresponder.is_enabled()
        first_enabled = self.autoresponder.is_first_message_enabled()
        triggers_count = len(self.autoresponder.get_triggers())
        review_enabled = self.autoresponder.is_review_responses_enabled()
        csv_enabled = self.autoresponder.is_csv_mode_enabled()
        
        text = f"{_('auto_title')}\n\n"
        text += f"{_('auto_status')} {_('enabled') if enabled else _('disabled')}\n"
        text += f"{_('auto_greeting')} {'✅' if first_enabled else '❌'}\n"
        text += f"{_('auto_triggers')} {triggers_count}\n"
        text += f"{_('auto_reviews')} {'✅' if review_enabled else '❌'}\n"
        text += f"{_('auto_csv')} {'✅' if csv_enabled else '❌'} ({len(self.autoresponder.get_csv_rules())})"
        
        keyboard = [
            [InlineKeyboardButton(f"{_('btn_turn_off') if enabled else _('btn_turn_on')}", callback_data="auto_toggle")],
            [InlineKeyboardButton(f"{_('btn_greeting')} {'✅' if first_enabled else '❌'}", callback_data="auto_first_toggle")],
            [InlineKeyboardButton(_("btn_greeting_text"), callback_data="auto_first_edit")],
            [InlineKeyboardButton(f"{_('btn_triggers')} ({triggers_count})", callback_data="auto_triggers")],
            [InlineKeyboardButton(f"{_('btn_reviews')} {'✅' if review_enabled else '❌'}", callback_data="auto_reviews")],
            [InlineKeyboardButton(f"{_('btn_csv')} {'✅' if csv_enabled else '❌'}", callback_data="csv_menu")],
            [InlineKeyboardButton(_("btn_back"), callback_data="menu")]
        ]
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)

    async def send_auto_menu_new(self, chat_id: int):
        """Отправить новое сообщение с меню автоответов (Translated)"""
        enabled = self.autoresponder.is_enabled()
        first_enabled = self.autoresponder.is_first_message_enabled()
        triggers_count = len(self.autoresponder.get_triggers())
        review_enabled = self.autoresponder.is_review_responses_enabled()
        csv_enabled = self.autoresponder.is_csv_mode_enabled()
        
        text = f"{_('auto_title')}\n\n"
        text += f"{_('auto_status')} {_('enabled') if enabled else _('disabled')}\n"
        text += f"{_('auto_greeting')} {'✅' if first_enabled else '❌'}\n"
        text += f"{_('auto_triggers')} {triggers_count}\n"
        text += f"{_('auto_reviews')} {'✅' if review_enabled else '❌'}\n"
        text += f"{_('auto_csv')} {'✅' if csv_enabled else '❌'} ({len(self.autoresponder.get_csv_rules())})"
        
        keyboard = [
            [InlineKeyboardButton(f"{_('btn_turn_off') if enabled else _('btn_turn_on')}", callback_data="auto_toggle")],
            [InlineKeyboardButton(f"{_('btn_greeting')} {'✅' if first_enabled else '❌'}", callback_data="auto_first_toggle")],
            [InlineKeyboardButton(_("btn_greeting_text"), callback_data="auto_first_edit")],
            [InlineKeyboardButton(f"{_('btn_triggers')} ({triggers_count})", callback_data="auto_triggers")],
            [InlineKeyboardButton(f"{_('btn_reviews')} {'✅' if review_enabled else '❌'}", callback_data="auto_reviews")],
            [InlineKeyboardButton(f"{_('btn_csv')} {'✅' if csv_enabled else '❌'}", callback_data="csv_menu")],
            [InlineKeyboardButton(_("btn_back"), callback_data="menu")]
        ]
        await self.telegram_bot.send_message_with_keyboard(text, keyboard, None)
         
    async def show_csv_menu(self, chat_id: int, message_id: int):
        """Показать меню режима ЧСВ."""
        enabled = self.autoresponder.is_csv_mode_enabled()
        rules = self.autoresponder.get_csv_rules()
        
        text = f"{_('csv_title')}\n\n"
        text += f"{_('auto_status')} ✅ {_('enabled') if enabled else '❌ ' + _('disabled')}\n\n"
        text += f"{_('csv_desc')}\n\n"
        
        if rules:
            text += f"📋 Rules ({len(rules)}):\n"
            for i, rule in enumerate(rules):
                status = "✅" if rule.get("enabled", True) else "❌"
                name = rule.get("option_name", "")[:15]
                value = rule.get("option_value", "")
                match_type = rule.get("match_type", "name")
                type_icon = {"name": "📝", "value": "🎯", "contains": "🔍"}.get(match_type, "📝")
                value_str = f"={value[:10]}" if value and match_type != "name" else ""
                to_user = "👤" if rule.get("send_to_user", False) else ""
                to_topic = "💬" if rule.get("send_to_topic", True) else ""
                text += f"{i+1}. {status} {type_icon} {name}{value_str} {to_user}{to_topic}\n"
        else:
            text += _('csv_rules_empty')
        
        keyboard = [
            [InlineKeyboardButton(f"{_('btn_turn_off') if enabled else _('btn_turn_on')}", callback_data="csv_toggle")],
        ]
        
        for i, rule in enumerate(rules):
            status = "✅" if rule.get("enabled", True) else "❌"
            name = rule.get("option_name", "")[:12]
            keyboard.append([
                InlineKeyboardButton(f"{status} {name}", callback_data=f"csv_rule_{i}"),
                InlineKeyboardButton(_("btn_delete"), callback_data=f"csv_del_{i}")
            ])
        
        keyboard.append([InlineKeyboardButton(_("btn_add_rule"), callback_data="csv_add_rule")])
        keyboard.append([InlineKeyboardButton(_("btn_back"), callback_data="auto_menu")])
        
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_csv_rule_menu(self, chat_id: int, message_id: int, idx: int):
        """Показать меню редактирования правила ЧСВ."""
        rule = self.autoresponder.get_csv_rule(idx)
        if not rule:
            await self.show_csv_menu(chat_id, message_id)
            return
        
        enabled = rule.get("enabled", True)
        option_name = rule.get("option_name", "")
        option_value = rule.get("option_value", "")
        match_type = rule.get("match_type", "name")
        case_sensitive = rule.get("case_sensitive", False)
        send_to_user = rule.get("send_to_user", False)
        user_message = rule.get("user_message", "")
        send_to_topic = rule.get("send_to_topic", True)
        topic_message = rule.get("topic_message", "")
        
        match_desc = {
            "name": _('csv_type_name'),
            "value": _('csv_type_value'),
            "contains": _('csv_type_contains')
        }.get(match_type, _('csv_type_name'))
        
        text = f"{_('csv_rule_title')} #{idx+1}\n\n"
        text += f"{_('csv_option')} {option_name}\n"
        if option_value:
            text += f"{_('csv_value')} {option_value}\n"
        text += f"{_('csv_type')} {match_desc}\n"
        text += f"{_('auto_status')} {'✅' if enabled else '❌'}\n"
        text += f"{_('csv_register')} {_('csv_reg_strict') if case_sensitive else _('csv_reg_any')}\n\n"
        
        text += f"{_('csv_to_user')} {'✅' if send_to_user else '❌'}\n"
        if send_to_user and user_message:
            text += f"   └ {user_message[:50]}{'...' if len(user_message) > 50 else ''}\n"
        text += f"{_('csv_to_topic')} {'✅' if send_to_topic else '❌'}\n"
        if send_to_topic and topic_message:
            text += f"   └ {topic_message[:50]}{'...' if len(topic_message) > 50 else ''}\n"
        
        keyboard = [
            [InlineKeyboardButton(f"{_('btn_turn_off') if enabled else _('btn_turn_on')}", callback_data=f"csv_toggle_{idx}")],
            [InlineKeyboardButton(_("btn_edit_name"), callback_data=f"csv_name_{idx}")],
            [InlineKeyboardButton(_("btn_edit_value"), callback_data=f"csv_value_{idx}")],
            [InlineKeyboardButton(f"🔎 {match_desc}", callback_data=f"csv_matchtype_{idx}")],
            [InlineKeyboardButton(f"{_('csv_register')} {_('csv_reg_strict') if case_sensitive else _('csv_reg_any')}", callback_data=f"csv_case_{idx}")],
            [InlineKeyboardButton(f"{_('csv_to_user')} {'✅' if send_to_user else '❌'}", callback_data=f"csv_touser_{idx}")],
        ]
        
        if send_to_user:
            keyboard.append([InlineKeyboardButton(_("btn_edit_user_msg"), callback_data=f"csv_usermsg_{idx}")])
        
        keyboard.append([InlineKeyboardButton(f"{_('csv_to_topic')} {'✅' if send_to_topic else '❌'}", callback_data=f"csv_totopic_{idx}")])
        
        if send_to_topic:
            keyboard.append([InlineKeyboardButton(_("btn_edit_topic_msg"), callback_data=f"csv_topicmsg_{idx}")])
        
        keyboard.append([InlineKeyboardButton(_("btn_delete"), callback_data=f"csv_del_{idx}")])
        keyboard.append([InlineKeyboardButton(_("btn_back"), callback_data="csv_menu")])
        
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_triggers_menu(self, chat_id: int, message_id: int):
        """Показать меню триггеров"""
        triggers = self.autoresponder.get_triggers()
        text = f"{_('triggers_title')}\n\n"
        keyboard = []
        for i, trigger in enumerate(triggers):
            phrase = trigger.get('phrase', '')[:15]
            enabled = trigger.get('enabled', True)
            notify = trigger.get('notify_group', False)
            status = "✅" if enabled else "❌"
            notify_icon = "🔔" if notify else ""
            text += f"{i+1}. {status}{notify_icon} \"{phrase}\"\n"
            keyboard.append([
                InlineKeyboardButton(f"{status}{notify_icon} {phrase}", callback_data=f"auto_trigger_edit_{i}"),
                InlineKeyboardButton(_("btn_delete"), callback_data=f"auto_trigger_del_{i}")
            ])
        if not triggers:
            text += _('triggers_empty')
        
        keyboard.append([InlineKeyboardButton(_("btn_add_trigger"), callback_data="auto_add_trigger")])
        keyboard.append([InlineKeyboardButton(_("btn_back"), callback_data="auto_menu")])
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_trigger_edit_menu(self, chat_id: int, message_id: int, idx: int):
        """Показать меню редактирования триггера"""
        trigger = self.autoresponder.get_trigger(idx)
        if not trigger:
            await self.show_triggers_menu(chat_id, message_id)
            return
        
        phrase = trigger.get('phrase', '')
        response = trigger.get('response', '')
        enabled = trigger.get('enabled', True)
        notify = trigger.get('notify_group', False)
        notify_text = trigger.get('notify_text', '')
        exact_match = trigger.get('exact_match', False)
        
        text = f"{_('trigger_edit_title')} #{idx+1}\n\n"
        text += f"{_('trigger_phrase')} {phrase}\n"
        text += f"{_('trigger_answer')} {response[:50]}{'...' if len(response) > 50 else ''}\n"
        text += f"{_('auto_status')} {'✅' if enabled else '❌'}\n"
        text += f"{_('trigger_mode')} {_('trigger_mode_exact') if exact_match else _('trigger_mode_contain')}\n"
        text += f"{_('trigger_notify')} {'✅' if notify else '❌'}\n"
        if notify:
            text += f"{_('trigger_notify_text')} {notify_text or '(по умолчанию)'}\n"
        
        keyboard = [
            [InlineKeyboardButton(f"{_('btn_turn_off') if enabled else _('btn_turn_on')}", callback_data=f"auto_trigger_toggle_{idx}")],
            [InlineKeyboardButton(_("btn_edit_phrase"), callback_data=f"auto_trigger_phrase_{idx}")],
            [InlineKeyboardButton(_("btn_edit_answer"), callback_data=f"auto_trigger_response_{idx}")],
            [InlineKeyboardButton(f"🎯 {_('trigger_mode_exact') if exact_match else _('trigger_mode_contain')}", callback_data=f"auto_trigger_exact_{idx}")],
            [InlineKeyboardButton(f"{_('trigger_notify')} {'✅' if notify else '❌'}", callback_data=f"auto_trigger_notify_{idx}")],
        ]
        if notify:
            keyboard.append([InlineKeyboardButton(_("btn_edit_notify_text"), callback_data=f"auto_trigger_notifytext_{idx}")])
        
        keyboard.append([InlineKeyboardButton(_("btn_delete"), callback_data=f"auto_trigger_del_{idx}")])
        keyboard.append([InlineKeyboardButton(_("btn_back"), callback_data="auto_triggers")])
        
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_reviews_menu(self, chat_id: int, message_id: int):
        """Показать меню автоответов на отзывы"""
        enabled = self.autoresponder.is_review_responses_enabled()
        good_enabled = self.autoresponder.is_good_review_response_enabled()
        bad_enabled = self.autoresponder.is_bad_review_response_enabled()
        good_text = self.autoresponder.get_good_review_text()
        bad_text = self.autoresponder.get_bad_review_text()
        
        text = f"{_('reviews_title')}\n\n"
        text += f"{_('auto_status')} {'✅' if enabled else '❌'}\n\n"
        text += f"{_('reviews_good')} {'✅' if good_enabled else '❌'}\n"
        text += f"{_('reviews_text')} {good_text[:50]}{'...' if len(good_text) > 50 else ''}\n\n"
        text += f"{_('reviews_bad')} {'✅' if bad_enabled else '❌'}\n"
        text += f"{_('reviews_text')} {bad_text[:50]}{'...' if len(bad_text) > 50 else ''}"
        
        keyboard = [
            [InlineKeyboardButton(f"{_('btn_turn_off') if enabled else _('btn_turn_on')}", callback_data="auto_reviews_toggle")],
            [InlineKeyboardButton(f"{_('reviews_good')} {'✅' if good_enabled else '❌'}", callback_data="auto_reviews_good_toggle")],
            [InlineKeyboardButton(_("btn_good_edit"), callback_data="auto_reviews_good_edit")],
            [InlineKeyboardButton(f"{_('reviews_bad')} {'✅' if bad_enabled else '❌'}", callback_data="auto_reviews_bad_toggle")],
            [InlineKeyboardButton(_("btn_bad_edit"), callback_data="auto_reviews_bad_edit")],
            [InlineKeyboardButton(_("btn_back"), callback_data="auto_menu")]
        ]
        
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def handle_text_input(self, chat_id: int, text: str):
        """Обработка текстового ввода для настроек"""
        if chat_id not in self.awaiting_input:
            return False
        
        input_type = self.awaiting_input[chat_id].get("type")
        
        if input_type == "first_message":
            self.autoresponder.set_first_message_text(text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="auto_menu")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Текст приветствия обновлён:\n\n{text}", keyboard, None
            )
            return True
        
        elif input_type == "trigger_phrase":
            notify_group = self.awaiting_input[chat_id].get("notify_group", False)
            self.awaiting_input[chat_id] = {"type": "trigger_response", "phrase": text, "notify_group": notify_group}
            
            keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="auto_triggers")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"Триггер: \"{text}\"\n\n✏️ Теперь отправьте текст ответа:", keyboard, None
            )
            return True
        
        elif input_type == "trigger_response":
            phrase = self.awaiting_input[chat_id].get("phrase", "")
            notify_group = self.awaiting_input[chat_id].get("notify_group", False)
            idx = self.autoresponder.add_trigger(phrase, text, notify_group)
            del self.awaiting_input[chat_id]
            
            if notify_group:
                # Если с уведомлением - спрашиваем текст уведомления
                self.awaiting_input[chat_id] = {"type": "new_trigger_notify_text", "trigger_idx": idx}
                keyboard = [[InlineKeyboardButton("⏭ Пропустить", callback_data=f"auto_trigger_edit_{idx}")]]
                await self.telegram_bot.send_message_with_keyboard(
                    f"✅ Триггер добавлен!\n\n✏️ Отправьте текст уведомления (или пропустите):", keyboard, None
                )
            else:
                keyboard = [[InlineKeyboardButton("◀️ К триггерам", callback_data="auto_triggers")]]
                await self.telegram_bot.send_message_with_keyboard(
                    f"✅ Триггер добавлен!\n\nФраза: \"{phrase}\"\nОтвет: \"{text}\"", keyboard, None
                )
            return True
        
        # Редактирование триггера - фраза
        elif input_type == "edit_trigger_phrase":
            idx = self.awaiting_input[chat_id].get("trigger_idx")
            self.autoresponder.update_trigger(idx, phrase=text.lower())
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"auto_trigger_edit_{idx}")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Фраза обновлена: \"{text}\"", keyboard, None
            )
            return True
        
        # Редактирование триггера - ответ
        elif input_type == "edit_trigger_response":
            idx = self.awaiting_input[chat_id].get("trigger_idx")
            self.autoresponder.update_trigger(idx, response=text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"auto_trigger_edit_{idx}")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Ответ обновлён: \"{text}\"", keyboard, None
            )
            return True
        
        # Редактирование триггера - текст уведомления
        elif input_type == "edit_trigger_notify_text":
            idx = self.awaiting_input[chat_id].get("trigger_idx")
            self.autoresponder.update_trigger(idx, notify_text=text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"auto_trigger_edit_{idx}")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Текст уведомления обновлён: \"{text}\"", keyboard, None
            )
            return True
        
        # Новый триггер - текст уведомления
        elif input_type == "new_trigger_notify_text":
            idx = self.awaiting_input[chat_id].get("trigger_idx")
            self.autoresponder.update_trigger(idx, notify_text=text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ К триггерам", callback_data="auto_triggers")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Текст уведомления установлен!", keyboard, None
            )
            return True
        
        # Текст ответа на хороший отзыв
        elif input_type == "edit_good_review_text":
            self.autoresponder.set_good_review_text(text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="auto_reviews")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Текст для хороших отзывов обновлён:\n\n{text}", keyboard, None
            )
            return True
        
        # Текст ответа на плохой отзыв
        elif input_type == "edit_bad_review_text":
            self.autoresponder.set_bad_review_text(text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="auto_reviews")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Текст для плохих отзывов обновлён:\n\n{text}", keyboard, None
            )
            return True
        
        # === ЧСВ режим ===
        
        # Название опции для нового правила
        elif input_type == "csv_option_name":
            self.awaiting_input[chat_id] = {"type": "csv_topic_message_new", "option_name": text}
            keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="csv_menu")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"Опция: {text}\n\n✏️ Введите сообщение для топика (или отправьте - чтобы пропустить):",
                keyboard, None
            )
            return True
        
        # Сообщение в топик для нового правила
        elif input_type == "csv_topic_message_new":
            option_name = self.awaiting_input[chat_id].get("option_name", "")
            topic_msg = text if text != "-" else ""
            self.awaiting_input[chat_id] = {"type": "csv_user_message_new", "option_name": option_name, "topic_message": topic_msg}
            keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="csv_menu")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✏️ Введите сообщение для юзера (или отправьте - чтобы пропустить):",
                keyboard, None
            )
            return True
        
        # Сообщение юзеру для нового правила
        elif input_type == "csv_user_message_new":
            option_name = self.awaiting_input[chat_id].get("option_name", "")
            topic_msg = self.awaiting_input[chat_id].get("topic_message", "")
            user_msg = text if text != "-" else ""
            
            # Создаём правило
            send_to_topic = bool(topic_msg)
            send_to_user = bool(user_msg)
            
            idx = self.autoresponder.add_csv_rule(
                option_name=option_name,
                case_sensitive=False,
                send_to_user=send_to_user,
                user_message=user_msg,
                send_to_topic=send_to_topic,
                topic_message=topic_msg
            )
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ К правилам", callback_data="csv_menu")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Правило ЧСВ добавлено!\n\n📝 Опция: {option_name}\n💬 В топик: {'✅' if send_to_topic else '❌'}\n👤 Юзеру: {'✅' if send_to_user else '❌'}",
                keyboard, None
            )
            return True
        
        # Редактирование названия опции
        elif input_type == "csv_edit_name":
            idx = self.awaiting_input[chat_id].get("rule_idx")
            self.autoresponder.update_csv_rule(idx, option_name=text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"csv_rule_{idx}")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Название опции обновлено: {text}", keyboard, None
            )
            return True
        
        # Редактирование значения опции
        elif input_type == "csv_option_value":
            idx = self.awaiting_input[chat_id].get("rule_idx")
            value = "" if text == "-" else text
            self.autoresponder.update_csv_rule(idx, option_value=value)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"csv_rule_{idx}")]]
            msg = f"✅ Значение опции: {value}" if value else "✅ Значение очищено (любое)"
            await self.telegram_bot.send_message_with_keyboard(msg, keyboard, None)
            return True
        
        # Редактирование сообщения юзеру
        elif input_type == "csv_user_message":
            idx = self.awaiting_input[chat_id].get("rule_idx")
            self.autoresponder.update_csv_rule(idx, user_message=text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"csv_rule_{idx}")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Сообщение юзеру обновлено!", keyboard, None
            )
            return True
        
        # Редактирование сообщения в топик
        elif input_type == "csv_topic_message":
            idx = self.awaiting_input[chat_id].get("rule_idx")
            self.autoresponder.update_csv_rule(idx, topic_message=text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"csv_rule_{idx}")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Сообщение в топик обновлено!", keyboard, None
            )
            return True
        
        return False

    async def sync_topics_with_purchases(self):
        """Синхронизация топиков с покупками - проверка удалённых и создание новых"""
        logging.info("Запуск синхронизации топиков с покупками...")
        
        if not await self.ensure_ggsel_auth():
            logging.error("Синхронизация: ошибка авторизации")
            return
        
        # Сначала проверяем удалённые топики
        await self.check_deleted_topics()
        
        # Получаем последние покупки
        loop = asyncio.get_event_loop()
        sales_data = await loop.run_in_executor(None, self.ggsel_api.get_last_sales, 30)
        
        if not sales_data or sales_data.get('retval') != 0:
            logging.warning("Не удалось получить список покупок")
            return
        
        # Собираем invoice_id из API
        api_invoice_ids = set()
        for sale in sales_data.get('sales', []):
            invoice_id = sale.get('invoice_id')
            if invoice_id:
                api_invoice_ids.add(invoice_id)
        
        # Существующие invoice_id в базе
        existing_invoice_ids = set()
        for key in self.topic_manager.topics.keys():
            if key.startswith('purchase_'):
                try:
                    existing_invoice_ids.add(int(key.replace('purchase_', '')))
                except:
                    pass
        
        logging.info(f"В базе: {len(existing_invoice_ids)} топиков, в API: {len(api_invoice_ids)} покупок")
        
        # Создаём топики для новых покупок
        missing_invoice_ids = api_invoice_ids - existing_invoice_ids
        
        if not missing_invoice_ids:
            return
        
        logging.info(f"Создаём {len(missing_invoice_ids)} новых топиков...")
        
        created_count = 0
        for invoice_id in missing_invoice_ids:
            purchase_data = await loop.run_in_executor(
                None, self.ggsel_api.get_purchase_info, invoice_id
            )
            
            if not purchase_data:
                continue
            
            purchase = self.purchase_manager.parse_purchase_response(purchase_data, invoice_id)
            if purchase:
                self.purchase_manager.add_purchase(purchase)
                await self.create_topic_for_purchase(purchase)
                created_count += 1
                
                if self.flood_control_until:
                    break
                    
                await asyncio.sleep(3)
        
        if created_count > 0:
            logging.info(f"Синхронизация: создано {created_count} топиков")
    
    async def check_deleted_topics(self):
        """Проверка и пересоздание удалённых топиков"""
        all_topics = self.topic_manager.get_all_topics()
        purchase_topics = {k: v for k, v in all_topics.items() if k.startswith('purchase_')}
        
        if not purchase_topics:
            return
        
        deleted_count = 0
        recreated_count = 0
        
        for topic_key, topic_info in list(purchase_topics.items()):
            topic_id = topic_info.get('topic_id')
            topic_name = topic_info.get('topic_name', '💬')
            invoice_id = topic_info.get('invoice_id')
            
            if not topic_id or not topic_name:
                continue
            
            # Проверяем существует ли топик (ставим то же название)
            exists = await self.telegram_bot.check_topic_exists(topic_id, topic_name)
            
            if not exists:
                deleted_count += 1
                logging.info(f"Топик {topic_id} (invoice {invoice_id}) удалён, пересоздаём...")
                
                # Удаляем из базы
                self.topic_manager.remove_topic(topic_key)
                
                # Пересоздаём топик
                if invoice_id:
                    loop = asyncio.get_event_loop()
                    purchase_data = await loop.run_in_executor(
                        None, self.ggsel_api.get_purchase_info, invoice_id
                    )
                    
                    if purchase_data:
                        purchase = self.purchase_manager.parse_purchase_response(purchase_data, invoice_id)
                        if purchase:
                            await self.create_topic_for_purchase(purchase, skip_greeting=True)
                            recreated_count += 1
                            
                            if self.flood_control_until:
                                break
                            
                            await asyncio.sleep(3)
            
            await asyncio.sleep(0.5)  # Небольшая задержка между проверками
        
        if deleted_count > 0:
            logging.info(f"Найдено удалённых: {deleted_count}, пересоздано: {recreated_count}")
    
    async def check_new_reviews(self):
        """Проверка новых отзывов - двойная проверка: общая по API + по invoice_id топиков"""
        try:
            loop = asyncio.get_event_loop()
            all_topics = self.topic_manager.get_all_topics()
            
            # Создаём маппинг invoice_id -> topic_info
            invoice_to_topic = {}
            for key, topic_info in all_topics.items():
                if key.startswith('purchase_'):
                    inv_id = topic_info.get('invoice_id')
                    if inv_id:
                        invoice_to_topic[int(inv_id)] = topic_info
            
            # Запускаем обе проверки параллельно
            await asyncio.gather(
                self._check_reviews_by_api(loop, invoice_to_topic),
                self._check_reviews_by_topics(loop, invoice_to_topic),
                return_exceptions=True
            )
                
        except Exception as e:
            logging.error(f"Ошибка проверки отзывов: {e}")

    async def _check_reviews_by_api(self, loop, invoice_to_topic: dict):
        """Проверка отзывов через общий API (пагинация)"""
        try:
            all_reviews = []
            max_known_id = max([int(rid) for rid in self.processed_reviews.keys()] or [0])
            
            for page in range(1, 6):
                reviews_data = await loop.run_in_executor(
                    _executor, lambda p=page: self.ggsel_api.get_reviews(50, page=p)
                )
                
                if not reviews_data:
                    break
                
                reviews = reviews_data.get('reviews', [])
                if not reviews:
                    break
                
                all_reviews.extend(reviews)
                
                page_ids = [int(r.get('id', 0)) for r in reviews if r.get('id')]
                if page_ids and max(page_ids) <= max_known_id and all(str(rid) in self.processed_reviews for rid in page_ids):
                    break
                
                await asyncio.sleep(0.3)
            
            if all_reviews:
                all_reviews.sort(key=lambda r: int(r.get('id', 0)), reverse=True)
                await self._process_reviews(all_reviews, invoice_to_topic, loop)
                
        except Exception as e:
            logging.error(f"Ошибка проверки отзывов по API: {e}")

    async def _check_reviews_by_topics(self, loop, invoice_to_topic: dict):
        """Проверка отзывов по invoice_id каждого топика (параллельно)"""
        try:
            # Проверяем все топики что есть в базе
            all_topics = list(invoice_to_topic.items())
            
            semaphore = asyncio.Semaphore(10)  # Максимум 10 параллельных запросов
            
            async def check_single_invoice(invoice_id: int, topic_info: dict):
                async with semaphore:
                    try:
                        review = await loop.run_in_executor(
                            _executor, lambda inv=invoice_id: self.ggsel_api.get_review_by_invoice(inv)
                        )
                        if review:
                            await self._process_reviews([review], {invoice_id: topic_info}, loop)
                    except Exception as e:
                        logging.debug(f"Ошибка проверки отзыва для {invoice_id}: {e}")
            
            tasks = [check_single_invoice(inv_id, info) for inv_id, info in all_topics]
            await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            logging.error(f"Ошибка проверки отзывов по топикам: {e}")

    async def _process_reviews(self, reviews: list, invoice_to_topic: dict, loop):
        """Обработка списка отзывов"""
        new_reviews_count = 0
        
        for review in reviews:
            review_id = str(review.get('id', ''))
            if not review_id:
                continue
            
            review_type = review.get('type', 'good')
            info = review.get('info', '') or review.get('text', '') or ''
            review_hash = f"{review_type}:{info}"
            
            old_hash = self.processed_reviews.get(review_id)
            if old_hash == review_hash:
                continue
            
            is_updated = old_hash is not None
            
            self.processed_reviews[review_id] = review_hash
            self._save_processed_reviews()
            
            invoice_id = review.get('invoice_id')
            if not invoice_id:
                continue
            
            topic_info = invoice_to_topic.get(int(invoice_id))
            if not topic_info:
                continue
            
            topic_id = topic_info.get('topic_id')
            if not topic_id:
                continue
            
            name = review.get('name', '')
            date = review.get('date', '')
            
            emoji = "👍" if review_type == 'good' else "👎"
            type_text = "Положительный" if review_type == 'good' else "Отрицательный"
            
            if is_updated:
                prefix = f"✏️ Отзыв изменён! {emoji}"
            else:
                prefix = f"{emoji} Новый отзыв!"
            
            msg = f"{prefix}\n"
            msg += f"📊 Тип: {type_text}\n"
            if name:
                msg += f"📦 {name}\n"
            if date:
                msg += f"📅 {date}\n"
            if info:
                msg += f"\n💬 {info}"
            
            await self.send_message_with_cooldown(msg, topic_id)
            new_reviews_count += 1
            logging.info(f"Отзыв {review_id} ({review_type}) -> топик {topic_id} (invoice {invoice_id})")
            
            auto_response = self.autoresponder.get_review_response(review_type)
            if auto_response:
                try:
                    await loop.run_in_executor(
                        _executor,
                        lambda cid=int(invoice_id), txt=auto_response: self.ggsel_api.send_message(cid, txt)
                    )
                    await self.send_message_with_cooldown(f"📤 {auto_response}", topic_id)
                except Exception as e:
                    logging.error(f"Ошибка автоответа на отзыв: {e}")
        
        if new_reviews_count > 0:
            logging.info(f"Обработано {new_reviews_count} новых отзывов")

    async def process_pending_history_loads(self):
        """Загрузка истории сообщений для созданных топиков"""
        if not self.pending_history_loads:
            return
        
        logging.info(f"Загружаем историю для {len(self.pending_history_loads)} топиков")
        
        loads = self.pending_history_loads.copy()
        self.pending_history_loads.clear()
        
        for item in loads:
            try:
                await self.load_chat_history(item['chat_ids'], item['topic_id'])
                await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"Ошибка загрузки истории для {item.get('invoice_id')}: {e}")

    async def handle_history_command(self, topic_id: int):
        """Обработка команды /history - загрузка истории в топик"""
        try:
            # Ищем топик по topic_id
            all_topics = self.topic_manager.get_all_topics()
            target_topic = None
            
            for key, info in all_topics.items():
                if info.get('topic_id') == topic_id:
                    target_topic = info
                    break
            
            if not target_topic:
                await self.telegram_bot.send_message("❌ Топик не найден в базе", topic_id)
                return
            
            invoice_id = target_topic.get('invoice_id')
            if not invoice_id:
                await self.telegram_bot.send_message("❌ Нет invoice_id", topic_id)
                return
            
            # НЕ удаляем обработанные сообщения, а добавляем флаг переотправки
            await self.telegram_bot.send_message("🔄 Загружаю историю...", topic_id)
            
            # Загружаем историю с флагом force_reload=True
            await self.load_chat_history([invoice_id], topic_id, force_reload=True)
            await self.telegram_bot.send_message("✅ История загружена", topic_id)
            
        except Exception as e:
            logging.error(f"Ошибка загрузки истории: {e}")
            await self.telegram_bot.send_message(f"❌ Ошибка: {e}", topic_id)
            
        except Exception as e:
            logging.error(f"Ошибка загрузки истории: {e}")
            await self.telegram_bot.send_message(f"❌ Ошибка: {e}", topic_id)

    async def get_purchase_options(self, invoice_id: int) -> Optional[str]:
        """Получить опции покупки в виде текста"""
        text, _ = await self.get_purchase_options_with_list(invoice_id)
        return text
    
    async def get_purchase_options_with_list(self, invoice_id: int) -> tuple:
        """Получить опции покупки в виде текста и списка"""
        try:
            loop = asyncio.get_event_loop()
            purchase_data = await loop.run_in_executor(
                None, self.ggsel_api.get_purchase_info, invoice_id
            )
            
            if not purchase_data or purchase_data.get('retval') != 0:
                return None, []
            
            content = purchase_data.get('content', {})
            options = content.get('options', [])
            
            if not options:
                return None, []
            
            # Форматируем опции
            lines = []
            for opt in options:
                name = opt.get('name', '')
                user_data = opt.get('user_data', '')
                if name and user_data:
                    lines.append(f"• {name}: {user_data}")
            
            text = "\n".join(lines) if lines else None
            return text, options
            
        except Exception as e:
            logging.error(f"Ошибка получения опций: {e}")
            return None, []
    
    async def process_csv_rules(self, invoice_id: int, topic_id: int, options: list):
        """Обработка правил ЧСВ для опций покупки"""
        try:
            results = self.autoresponder.check_csv_options(options)
            
            if not results:
                return
            
            loop = asyncio.get_event_loop()
            
            for result in results:
                option = result.get("option", {})
                option_name = option.get("name", "")
                option_value = option.get("user_data", "")
                
                # Отправляем в топик
                if result.get("send_to_topic") and result.get("topic_message"):
                    topic_msg = result["topic_message"]
                    # Подставляем переменные
                    topic_msg = topic_msg.replace("{option}", option_name)
                    topic_msg = topic_msg.replace("{value}", option_value)
                    topic_msg = topic_msg.replace("{sum}", option_value)  # алиас для {value}
                    await self.send_message_with_cooldown(f"🎯 {topic_msg}", topic_id)
                    logging.info(f"ЧСВ: отправлено в топик для опции '{option_name}'")
                
                # Отправляем юзеру
                if result.get("send_to_user") and result.get("user_message"):
                    user_msg = result["user_message"]
                    # Подставляем переменные
                    user_msg = user_msg.replace("{option}", option_name)
                    user_msg = user_msg.replace("{value}", option_value)
                    user_msg = user_msg.replace("{sum}", option_value)  # алиас для {value}
                    try:
                        await loop.run_in_executor(
                            None,
                            lambda cid=invoice_id, msg=user_msg: self.ggsel_api.send_message(cid, msg)
                        )
                        await self.send_message_with_cooldown(f"📤 {user_msg}", topic_id)
                        logging.info(f"ЧСВ: отправлено юзеру для опции '{option_name}'")
                    except Exception as e:
                        logging.error(f"ЧСВ: ошибка отправки юзеру: {e}")
                        
        except Exception as e:
            logging.error(f"Ошибка обработки ЧСВ: {e}")
    
    async def handle_options_command(self, topic_id: int):
        """Обработка команды /options - показать опции покупки"""
        try:
            all_topics = self.topic_manager.get_all_topics()
            target_topic = None
            
            for key, info in all_topics.items():
                if info.get('topic_id') == topic_id:
                    target_topic = info
                    break
            
            if not target_topic:
                await self.telegram_bot.send_message("❌ Топик не найден", topic_id)
                return
            
            invoice_id = target_topic.get('invoice_id')
            if not invoice_id:
                await self.telegram_bot.send_message("❌ Нет invoice_id", topic_id)
                return
            
            options_text = await self.get_purchase_options(invoice_id)
            
            if options_text:
                msg = f"⚙️ Опции покупки #{invoice_id}:\n\n{options_text}"
            else:
                msg = f"ℹ️ Нет опций для покупки #{invoice_id}"
            
            await self.telegram_bot.send_message(msg, topic_id)
            
        except Exception as e:
            logging.error(f"Ошибка команды /options: {e}")
            await self.telegram_bot.send_message(f"❌ Ошибка: {e}", topic_id)

    async def handle_review_command(self, topic_id: int):
        """Обработка команды /review - проверить отзыв по invoice_id"""
        try:
            all_topics = self.topic_manager.get_all_topics()
            target_topic = None
            
            for key, info in all_topics.items():
                if info.get('topic_id') == topic_id:
                    target_topic = info
                    break
            
            if not target_topic:
                await self.telegram_bot.send_message("❌ Топик не найден", topic_id)
                return
            
            invoice_id = target_topic.get('invoice_id')
            if not invoice_id:
                await self.telegram_bot.send_message("❌ Нет invoice_id", topic_id)
                return
            
            await self.telegram_bot.send_message(f"🔍 Ищу отзыв для #{invoice_id}...", topic_id)
            
            loop = asyncio.get_event_loop()
            review = await loop.run_in_executor(
                _executor, lambda: self.ggsel_api.get_review_by_invoice(invoice_id)
            )
            
            if review:
                review_type = review.get('type', 'good')
                emoji = "👍" if review_type == 'good' else "👎"
                info = review.get('info', '') or ''
                name = review.get('name', '')
                date = review.get('date', '')
                review_id = review.get('id', '')
                
                msg = f"{emoji} Отзыв найден!\n\n"
                msg += f"🆔 ID: {review_id}\n"
                msg += f"📦 {name}\n" if name else ""
                msg += f"📅 {date}\n" if date else ""
                msg += f"📝 Тип: {review_type}\n"
                if info:
                    msg += f"\n💬 {info}"
                
                await self.telegram_bot.send_message(msg, topic_id)
            else:
                await self.telegram_bot.send_message(f"ℹ️ Отзыв для #{invoice_id} не найден", topic_id)
            
        except Exception as e:
            logging.error(f"Ошибка команды /review: {e}")
            await self.telegram_bot.send_message(f"❌ Ошибка: {e}", topic_id)
