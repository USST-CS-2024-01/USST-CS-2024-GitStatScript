import json
import logging

import pymysql
from kafka import KafkaConsumer

import git_clone
from config import KAFKA_TOPIC, KAFKA_HOST, KAFKA_PORT, MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, \
    MYSQL_DATABASE

consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}")
mysql_conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD,
                             database=MYSQL_DATABASE)


def get_ssh_private_key():
    sql = "SELECT value FROM config WHERE `key` = 'git:ssh_private_key'"
    with mysql_conn.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            logging.error("SSH private key not found")
            return None


def update_task_status(task_id, status, commit_stats: {}, code_line_stats: {}):
    sql = "UPDATE repo_record SET status = %s, commit_stats = %s, code_line_stats = %s, stat_time=NOW() WHERE id = %s"
    with mysql_conn.cursor() as cursor:
        cursor.execute(sql, (status, json.dumps(commit_stats), json.dumps(code_line_stats), task_id))
    mysql_conn.commit()


def create_file_and_update(task_id, file_key, file_size):
    file_name = file_key.split('/')[-1]
    sql = """INSERT INTO file (name, file_key, file_type, file_size, owner_type, owner_user_id,
    create_date, modify_date, tags) VALUES (%s, %s, 'other', %s, 'user', 1, NOW(), NOW(), '["repo_archive"]')"""
    sql2 = """UPDATE repo_record SET archive_file_id = %s WHERE id = %s"""
    with mysql_conn.cursor() as cursor:
        cursor.execute(sql, (file_name, file_key, file_size))
        file_id = cursor.lastrowid
        cursor.execute(sql2, (file_id, task_id))
    mysql_conn.commit()


def start_task(message):
    try:
        data = message.decode('utf-8')
        data = json.loads(data)
        task_id = data['id']
    except Exception as e:
        logging.error(f"Error processing message [{message}]: {e}")
        return

    logging.info(f"Starting task {task_id}")
    git_clone.clone_repository(data['repo_url'], task_id, get_ssh_private_key())
    try:
        code_contributions, commit_contributions = git_clone.analyze_contributions(f'tmp/{task_id}')
        update_task_status(task_id, 'completed', commit_contributions, code_contributions)
        file_size = git_clone.pack_zip(task_id, data['upload_url'], data['complete_url'])
        create_file_and_update(task_id, data['file_key'], file_size)
    except Exception as e:
        logging.error(f"Error analyzing contributions: {e}")
        update_task_status(task_id, 'failed', {}, {})
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
