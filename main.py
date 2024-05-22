import json
import logging
import pymysql
from kafka import KafkaConsumer
import git_clone
from config import KAFKA_TOPIC, KAFKA_HOST, KAFKA_PORT, MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, \
    MYSQL_DATABASE

# Initialize Kafka consumer
consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}")


# Function to establish a MySQL connection
def get_mysql_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )


# Initialize MySQL connection
mysql_conn = get_mysql_connection()


def execute_with_reconnect(func):
    def wrapper(*args, **kwargs):
        global mysql_conn
        try:
            return func(*args, **kwargs)
        except pymysql.MySQLError as e:
            logging.error(f"MySQL error: {e}. Attempting to reconnect.")
            mysql_conn = get_mysql_connection()
            return func(*args, **kwargs)

    return wrapper


@execute_with_reconnect
def get_ssh_private_key():
    try:
        sql = "SELECT value FROM config WHERE `key` = 'git:ssh_private_key'"
        with mysql_conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                logging.error("SSH private key not found")
                return None
    except Exception as e:
        logging.error(f"Error fetching SSH private key: {e}")
        return None


@execute_with_reconnect
def update_task_status(task_id, status, commit_stats, code_line_stats):
    try:
        sql = "UPDATE repo_record SET status = %s, commit_stats = %s, code_line_stats = %s, stat_time=NOW() WHERE id = %s"
        with mysql_conn.cursor() as cursor:
            cursor.execute(sql, (status, json.dumps(commit_stats), json.dumps(code_line_stats), task_id))
        mysql_conn.commit()
    except Exception as e:
        logging.error(f"Error updating task status for {task_id}: {e}")


@execute_with_reconnect
def create_file_and_update(task_id, file_key, file_size):
    try:
        file_name = file_key.split('/')[-1]
        sql = """INSERT INTO file (name, file_key, file_type, file_size, owner_type, owner_user_id, create_date, modify_date, tags) 
                 VALUES (%s, %s, 'other', %s, 'user', 1, NOW(), NOW(), '["repo_archive"]')"""
        sql2 = "UPDATE repo_record SET archive_file_id = %s WHERE id = %s"
        with mysql_conn.cursor() as cursor:
            cursor.execute(sql, (file_name, file_key, file_size))
            file_id = cursor.lastrowid
            cursor.execute(sql2, (file_id, task_id))
        mysql_conn.commit()
    except Exception as e:
        logging.error(f"Error creating file and updating record for task {task_id}: {e}")


def start_task(message):
    try:
        data = message.decode('utf-8')
        data = json.loads(data)
        task_id = data['id']
    except Exception as e:
        logging.error(f"Error processing message [{message}]: {e}")
        return

    logging.info(f"Starting task {task_id}")
    ssh_key = get_ssh_private_key()
    if not ssh_key:
        update_task_status(task_id, 'failed', {}, {"error": "SSH private key not found"})
        return

    git_clone.clone_repository(data['repo_url'], task_id, ssh_key)
    try:
        code_contributions, commit_contributions = git_clone.analyze_contributions(f'tmp/{task_id}')
        update_task_status(task_id, 'completed', commit_contributions, code_contributions)
        file_size = git_clone.pack_zip(task_id, data['upload_url'], data['complete_url'])
        create_file_and_update(task_id, data['file_key'], file_size)
    except Exception as e:
        logging.error(f"Error analyzing contributions: {e}")
        update_task_status(task_id, 'failed', {}, {"error": str(e)})
        return
    finally:
        git_clone.cleanup(task_id)
    logging.info(f"Task {task_id} completed")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    while True:
        for msg in consumer:
            try:
                start_task(msg.value)
            except Exception as e:
                logging.error(f"Error processing message [{msg.value}]: {e}")
