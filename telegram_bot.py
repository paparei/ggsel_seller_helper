import asyncio
import logging
import re
from typing import Optional, Callable, Tuple, List
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup, ReactionTypeEmoji
from telegram.ext import Application, MessageHandler, CallbackQueryHandler, CommandHandler, filters
from telegram.error import TelegramError
from config import Config
from locales import locales, _ # <-- Imported translation system

class TelegramBot:
    def __init__(self, config: Config):
        self.config = config
        self.bot = Bot(token=config.telegram_bot_token)
        self.group_id = config.telegram_group_id
        self.application = None
        self.topic_message_handler = None
        self.callback_handler = None 
        self.general_message_handler = None
        self.history_handler = None 
        self.options_handler = None 
        self.review_handler = None 
        
    async def start(self):
        try:
            bot_info = await self.bot.get_me()
            logging.info(f"Telegram Bot: @{bot_info.username}")
            
            self.application = (
                Application.builder()
                .token(self.config.telegram_bot_token)
                .connect_timeout(30)
                .read_timeout(30)
                .write_timeout(30)
                .build()
            )
            
            self.application.add_handler(CommandHandler("menu", self._handle_menu_command))
            self.application.add_handler(CommandHandler("auto", self._handle_auto_command))
            self.application.add_handler(CommandHandler("history", self._handle_history_command))
            self.application.add_handler(CommandHandler("options", self._handle_options_command))
            self.application.add_handler(CommandHandler("review", self._handle_review_command))
            self.application.add_handler(CallbackQueryHandler(self._handle_callback))
            
            if self.topic_message_handler:
                topic_filter = filters.Chat(chat_id=self.group_id) & filters.TEXT & filters.IS_TOPIC_MESSAGE
                self.application.add_handler(MessageHandler(topic_filter, self._handle_topic_message))
            
            general_filter = filters.Chat(chat_id=self.group_id) & filters.TEXT & ~filters.IS_TOPIC_MESSAGE & ~filters.COMMAND
            self.application.add_handler(MessageHandler(general_filter, self._handle_general_message))
            
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            return True
        except Exception as e:
            logging.error(f"Telegram init error: {e}")
            return False
    
    async def stop(self):
        if self.application:
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
    
    def set_topic_message_handler(self, handler: Callable): self.topic_message_handler = handler
    def set_callback_handler(self, handler: Callable): self.callback_handler = handler
    def set_general_message_handler(self, handler: Callable): self.general_message_handler = handler
    def set_history_handler(self, handler: Callable): self.history_handler = handler
    def set_options_handler(self, handler: Callable): self.options_handler = handler
    def set_review_handler(self, handler: Callable): self.review_handler = handler
    
    async def _handle_menu_command(self, update: Update, context):
        """Main Menu with translations"""
        if update.effective_chat.id != self.group_id: return
        
        keyboard = [
            [InlineKeyboardButton(_("btn_auto"), callback_data="auto_menu")],
            [InlineKeyboardButton(_("btn_stats"), callback_data="stats")],
            [InlineKeyboardButton(_("btn_lang"), callback_data="lang_toggle")],
            [InlineKeyboardButton(_("btn_close"), callback_data="close")]
        ]
        await update.message.reply_text(_("menu_title"), reply_markup=InlineKeyboardMarkup(keyboard))
    
    async def _handle_auto_command(self, update: Update, context):
        if update.effective_chat.id != self.group_id: return
        if self.callback_handler:
            await self.callback_handler("auto_menu_new", update, context)
            
    async def _handle_history_command(self, update: Update, context):
        if update.effective_chat.id != self.group_id: return
        topic_id = update.message.message_thread_id
        if not topic_id: return
        if self.history_handler:
            await self.history_handler(topic_id)
            
    async def _handle_options_command(self, update: Update, context):
        if update.effective_chat.id != self.group_id: return
        topic_id = update.message.message_thread_id
        if not topic_id: return
        if self.options_handler:
            await self.options_handler(topic_id)
            
    async def _handle_review_command(self, update: Update, context):
        if update.effective_chat.id != self.group_id: return
        topic_id = update.message.message_thread_id
        if not topic_id: return
        if self.review_handler:
            await self.review_handler(topic_id)
            
    async def _handle_callback(self, update: Update, context):
        query = update.callback_query
        await query.answer()
        if query.message.chat.id != self.group_id: return
        
        data = query.data
        if data == "close":
            await query.message.delete()
            return
            
        # Global language toggle handling intercept
        if data == "lang_toggle":
            locales.toggle()
            keyboard = [
                [InlineKeyboardButton(_("btn_auto"), callback_data="auto_menu")],
                [InlineKeyboardButton(_("btn_stats"), callback_data="stats")],
                [InlineKeyboardButton(_("btn_lang"), callback_data="lang_toggle")],
                [InlineKeyboardButton(_("btn_close"), callback_data="close")]
            ]
            await self.edit_message(query.message.message_id, query.message.chat.id, _("menu_title"), keyboard)
            return

        if self.callback_handler:
            await self.callback_handler(data, update, context)
            
    async def _handle_topic_message(self, update: Update, context):
        try:
            if update.message and update.message.text and not update.message.from_user.is_bot:
                text = update.message.text
                topic_id = update.message.message_thread_id
                username = update.message.from_user.username or "User"
                if self.topic_message_handler:
                    self.topic_message_handler(topic_id, text, username, update.message.message_id)
        except Exception as e: logging.error(f"Topic handler error: {e}")
        
    async def _handle_general_message(self, update: Update, context):
        try:
            if update.message and update.message.text and not update.message.from_user.is_bot:
                if self.general_message_handler:
                    await self.general_message_handler(update.message.text)
        except Exception as e: logging.error(f"General message error: {e}")

    # Core bot actions
    async def create_topic(self, topic_name: str) -> Tuple[Optional[int], Optional[int]]:
        topic_name = topic_name[:120] + "..." if len(topic_name) > 120 else topic_name
        try:
            result = await self.bot.create_forum_topic(chat_id=self.group_id, name=topic_name)
            return result.message_thread_id, None
        except Exception: return None, 60

    async def send_message(self, text: str, topic_id: int) -> Tuple[bool, Optional[int]]:
        text = text[:4000] + "..." if len(text) > 4000 else text
        try:
            if topic_id == -1: await self.bot.send_message(chat_id=self.group_id, text=text)
            else: await self.bot.send_message(chat_id=self.group_id, message_thread_id=topic_id, text=text)
            return True, None
        except Exception: return False, 60
        
    async def send_message_with_keyboard(self, text: str, keyboard: list, topic_id: int = None) -> bool:
        try:
            reply_markup = InlineKeyboardMarkup(keyboard)
            if topic_id and topic_id != -1:
                await self.bot.send_message(chat_id=self.group_id, message_thread_id=topic_id, text=text, reply_markup=reply_markup)
            else:
                await self.bot.send_message(chat_id=self.group_id, text=text, reply_markup=reply_markup)
            return True
        except Exception: return False
        
    async def edit_message(self, message_id: int, chat_id: int, text: str, keyboard: list = None):
        try:
            reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
            await self.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=reply_markup)
            return True
        except Exception: return False
        
    async def add_reaction(self, message_id: int, topic_id: int, emoji: str = "🔥") -> bool:
        try:
            await self.bot.set_message_reaction(chat_id=self.group_id, message_id=message_id, reaction=[ReactionTypeEmoji(emoji=emoji)])
            return True
        except Exception: return False

    async def check_topic_exists(self, topic_id: int, topic_name: str) -> bool:
        try:
            await self.bot.edit_forum_topic(chat_id=self.group_id, message_thread_id=topic_id, name=topic_name)
            return True
        except Exception as e:
            return False if any(err in str(e).lower() for err in ["deleted", "not found", "invalid"]) else True
