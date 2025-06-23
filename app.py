import os
import time
import logging
import pyodbc
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseTelegramBot:
    def __init__(self):
        # Database configuration
        self.db_server = os.getenv('DB_SERVER')
        self.db_database = os.getenv('DB_DATABASE')
        self.db_username = os.getenv('DB_USERNAME') 
        self.db_password = os.getenv('DB_PASSWORD')
        self.db_view = os.getenv('DB_VIEW', 'your_view_name')
        self.db_table = os.getenv('DB_TABLE', 'your_table_name')
        
        # Telegram configuration
        self.telegram_token = os.getenv('TELEGRAM_BOT_TOKEN')
        self.telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # Polling configuration
        self.poll_interval = int(os.getenv('POLL_INTERVAL', '30'))  # seconds
        
        # Validate required environment variables
        self._validate_config()
        
    def _validate_config(self):
        """Validate all required configuration is present"""
        required_vars = [
            'DB_SERVER', 'DB_DATABASE', 'DB_USERNAME', 'DB_PASSWORD',
            'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
            
    def get_db_connection(self):
        """Create and return database connection"""
        try:
            connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.db_server};"
                f"DATABASE={self.db_database};"
                f"UID={self.db_username};"
                f"PWD={self.db_password};"
                f"TrustServerCertificate=yes;"
            )
            
            conn = pyodbc.connect(connection_string)
            logger.info("Database connection established successfully")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def fetch_pending_messages(self):
        """Fetch records from the database view"""
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Modify this query based on your view structure
            query = f"SELECT id, message_text, recipient, created_at FROM {self.db_view}"
            cursor.execute(query)
            
            records = cursor.fetchall()
            cursor.close()
            conn.close()
            
            logger.info(f"Fetched {len(records)} pending messages")
            return records
            
        except Exception as e:
            logger.error(f"Error fetching messages: {e}")
            return []
            
    def send_telegram_message(self, message_text, chat_id=None):
        """Send message via Telegram Bot API"""
        try:
            chat_id = chat_id or self.telegram_chat_id
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            
            payload = {
                'chat_id': chat_id,
                'text': message_text,
                'parse_mode': 'HTML'  # Enable HTML formatting
            }
            
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            logger.info(f"Message sent successfully to chat {chat_id}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False
            
    def delete_processed_records(self, record_ids):
        """Delete processed records from the table"""
        if not record_ids:
            return
            
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            
            # Create placeholders for the IN clause
            placeholders = ','.join(['?' for _ in record_ids])
            delete_query = f"DELETE FROM {self.db_table} WHERE id IN ({placeholders})"
            
            cursor.execute(delete_query, record_ids)
            conn.commit()
            
            deleted_count = cursor.rowcount
            cursor.close()
            conn.close()
            
            logger.info(f"Deleted {deleted_count} processed records")
            
        except Exception as e:
            logger.error(f"Error deleting processed records: {e}")
            
    def format_message(self, record):
        """Format database record into Telegram message"""
        # Customize this method based on your data structure
        message_parts = []
        
        if len(record) >= 4:
            record_id, message_text, recipient, created_at = record[:4]
            
            message_parts.append(f"<b>📨 New Message</b>")
            message_parts.append(f"<b>ID:</b> {record_id}")
            message_parts.append(f"<b>To:</b> {recipient}")
            message_parts.append(f"<b>Message:</b> {message_text}")
            message_parts.append(f"<b>Time:</b> {created_at}")
        else:
            message_parts.append(f"<b>📨 Record:</b> {str(record)}")
            
        return '\n'.join(message_parts)
        
    def process_messages(self):
        """Main processing loop for handling messages"""
        try:
            records = self.fetch_pending_messages()
            
            if not records:
                logger.debug("No pending messages found")
                return
                
            successful_ids = []
            
            for record in records:
                try:
                    formatted_message = self.format_message(record)
                    
                    if self.send_telegram_message(formatted_message):
                        # Assuming first column is the ID
                        successful_ids.append(record[0])
                    else:
                        logger.warning(f"Failed to send message for record ID: {record[0]}")
                        
                    # Add small delay between messages to avoid rate limiting
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error processing record {record[0]}: {e}")
                    
            # Delete successfully processed records
            if successful_ids:
                self.delete_processed_records(successful_ids)
                logger.info(f"Processing cycle completed. Sent {len(successful_ids)} messages")
                
        except Exception as e:
            logger.error(f"Error in process_messages: {e}")
            
    def run(self):
        """Main application loop"""
        logger.info("Starting Database-Telegram Bot")
        logger.info(f"Polling interval: {self.poll_interval} seconds")
        
        while True:
            try:
                self.process_messages()
                time.sleep(self.poll_interval)
                
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                time.sleep(self.poll_interval)
                
        logger.info("Application stopped")

if __name__ == "__main__":
    bot = DatabaseTelegramBot()
    bot.run()
