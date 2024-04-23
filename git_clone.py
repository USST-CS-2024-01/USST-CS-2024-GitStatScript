import logging
import os
import shutil
from collections import defaultdict

import requests
from git import Repo

try:
    os.mkdir('tmp')
except FileExistsError:
    pass


def clone_repository(git_url, task_id: int, private_key: str):
    # 设置使用的SSH密钥
    with open('/tmp/private_key', 'w') as f:
        f.write(private_key)
    os.chmod('/tmp/private_key', 0o600)
    os.environ['GIT_SSH_COMMAND'] = 'ssh -i /tmp/private_key -o IdentitiesOnly=yes -o StrictHostKeyChecking=no'

    directory = f'tmp/{task_id}'
    try:
        Repo.clone_from(git_url, directory)
        logging.info(f"Cloned repository {git_url} to {directory}")
    except Exception as e:
        logging.error(f"Error cloning repository {git_url}: {e}")

    try:
        os.remove('tmp/private_key')
    except Exception as _:
        pass


def analyze_contributions(repo_path):
    # 加载仓库
    repo = Repo(repo_path)

    # 初始化数据结构来存储统计数据
    stats = defaultdict(lambda: {'commits': 0, 'additions': 0, 'deletions': 0})

    # 遍历仓库的所有commit
    for commit in repo.iter_commits():
        author = commit.author.email  # 或者使用 commit.author.name
        stats[author]['commits'] += 1
        # 获取每个commit的统计信息
        try:
            diff = commit.stats.total
            stats[author]['additions'] += diff['insertions']
            stats[author]['deletions'] += diff['deletions']
        except Exception as e:
            raise RuntimeError(f"Error processing: {e}")

    # 输出统计结果
    code_contributions = {}
    commit_contributions = {}
    for author, stat in stats.items():
        code_contributions[author] = stat['additions'] + stat['deletions']
        commit_contributions[author] = stat['commits']

    repo.close()

    return code_contributions, commit_contributions


def pack_zip(task_id, upload_url, complete_url):
    shutil.make_archive(f'tmp/{task_id}', 'zip', f'tmp/{task_id}')
    file_size = os.path.getsize(f'tmp/{task_id}.zip')
    ret = requests.put(upload_url, data=open(f'tmp/{task_id}.zip', 'rb'))
    if ret.status_code >= 400:
        raise RuntimeError(f"Error uploading zip: {ret.text}")
    else:
        logging.info(f"Uploaded zip to {upload_url}")
    ret = requests.post(complete_url)
    if ret.status_code >= 400:
        raise RuntimeError(f"Error completing upload: {ret.text}")
    else:
        logging.info(f"Completed upload at {complete_url}")
    try:
        os.remove(f'tmp/{task_id}.zip')
    except Exception as _:
        pass
    return file_size


def cleanup(task_id):
    try:
        shutil.rmtree(f'tmp/{task_id}')
    except Exception as e:
        logging.error(f"Error cleaning up: {e}")
