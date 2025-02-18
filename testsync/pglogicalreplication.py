import psycopg2
import time
import logging
from psycopg2 import sql, OperationalError
from psycopg2.extras import LogicalReplicationConnection
from threading import Event

class LogicalReplicator:
    def __init__(self, publisher_dsn, slot_name='pydemo_slot', publication_name='pydemo_pub'):
        self.publisher_dsn = publisher_dsn
        self.slot_name = slot_name
        self.publication_name = publication_name
        self.conn = None
        self.cursor = None
        self.last_lsn = self.load_last_lsn()  # Load persisted LSN
        self.shutdown_flag = Event()
        self.retry_delay = 5  # Initial retry delay in seconds
        self.max_retry_delay = 60  # Maximum retry delay
        self.logger = self.setup_logger()

    def setup_logger(self):
        logger = logging.getLogger('LogicalReplicator')
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def connect(self):
        """Establish/re-establish connection to PostgreSQL publisher"""
        attempts = 0
        max_attempts = 5
        
        while not self.shutdown_flag.is_set() and attempts < max_attempts:
            try:
                self.conn = psycopg2.connect(
                    self.publisher_dsn,
                    connection_factory=LogicalReplicationConnection
                )
                self.cursor = self.conn.cursor()
                self.logger.info("Connected to PostgreSQL server")
                return True
            except OperationalError as e:
                self.logger.error(f"Connection failed (attempt {attempts+1}/{max_attempts}): {e}")
                time.sleep(self.retry_delay)
                attempts += 1
                self.retry_delay = min(self.retry_delay * 2, self.max_retry_delay)
        
        self.logger.error("Failed to establish connection after multiple attempts")
        return False

    def create_replication_slot(self):
        """Ensure replication slot exists"""
        try:
            self.cursor.create_replication_slot(
                self.slot_name, output_plugin='pgoutput')
            self.logger.info(f"Created replication slot: {self.slot_name}")
        except psycopg2.ProgrammingError as e:
            if "already exists" in str(e):
                self.logger.info(f"Replication slot {self.slot_name} exists")
            else:
                self.logger.error(f"Error creating replication slot: {e}")
                raise

    def start_replication(self):
        """Start/resume replication from last known LSN"""
        options = {
            'publication_names': self.publication_name,
            'proto_version': '1'
        }
        
        try:
            self.cursor.start_replication(
                slot_name=self.slot_name,
                decode=True,
                options=options,
                status_interval=10,
                start_lsn=self.last_lsn  # Resume from last known position
            )
            self.logger.info(f"Started replication from LSN {self.last_lsn}")
            self.retry_delay = 5  # Reset retry delay on success
            return True
        except psycopg2.OperationalError as e:
            self.logger.error(f"Failed to start replication: {e}")
            return False

    def run_continuous_replication(self):
        """Main loop with automatic recovery"""
        while not self.shutdown_flag.is_set():
            try:
                if self.connect() and self.start_replication():
                    self.process_replication_stream()
                else:
                    self.logger.error("Retrying connection in %d seconds...", self.retry_delay)
                    time.sleep(self.retry_delay)
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}", exc_info=True)
                self.logger.info(f"Reconnecting in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
                self.retry_delay = min(self.retry_delay * 2, self.max_retry_delay)
            finally:
                self.stop()

    def process_replication_stream(self):
        """Process incoming replication messages"""
        self.logger.info("Starting replication stream...")
        try:
            self.cursor.consume_stream(self.handle_message)
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal")
            self.shutdown_flag.set()
        except Exception as e:
            self.logger.error(f"Replication stream error: {e}")
            raise

    def handle_message(self, msg):
        """Handle incoming replication messages"""
        try:
            self.last_lsn = msg.data_start
            payload = msg.payload
            
            if isinstance(payload, dict):
                self.process_change_message(payload)
            elif msg.payload == 'BEGIN':
                self.logger.debug(f"Transaction BEGIN at {msg.data_start}")
            elif msg.payload == 'COMMIT':
                self.logger.debug(f"Transaction COMMIT at {msg.data_start}")

            # Persist LSN periodically
            if msg.data_start % 100 == 0:  # Adjust based on your needs
                self.persist_last_lsn()

            self.send_feedback()
        except Exception as e:
            self.logger.error(f"Error processing message: {e}", exc_info=True)
            raise

    def persist_last_lsn(self):
        """Save LSN to persistent storage"""
        # Implement your persistence mechanism (file, database, etc.)
        # Example: write to file
        with open('last_lsn', 'w') as f:
            f.write(str(self.last_lsn))
        self.logger.debug(f"Persisted LSN: {self.last_lsn}")

    def load_last_lsn(self):
        """Load LSN from persistent storage"""
        try:
            with open('last_lsn', 'r') as f:
                return int(f.read())
        except (FileNotFoundError, ValueError):
            return 0  # Start from beginning if no LSN found

    def send_feedback(self):
        """Send replication feedback to server"""
        if self.last_lsn:
            try:
                self.cursor.send_feedback(flush_lsn=self.last_lsn)
            except psycopg2.InterfaceError as e:
                self.logger.error(f"Feedback error: {e}")
                raise

    def stop(self):
        """Clean up resources"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            self.persist_last_lsn()
            self.logger.info("Resources cleaned up and LSN persisted")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

if __name__ == '__main__':
    replicator = LogicalReplicator(
        publisher_dsn="dbname='postgres' user='postgres' host='localhost' password='secret'",
        slot_name='py_slot',
        publication_name='py_pub'
    )
    
    try:
        replicator.logger.info("Starting replication service")
        replicator.run_continuous_replication()
    except KeyboardInterrupt:
        replicator.logger.info("Shutting down gracefully")
        replicator.shutdown_flag.set()
    finally:
        replicator.stop()