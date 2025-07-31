import os

import duckdb
from paramiko import SSHClient, SSHConfig, AutoAddPolicy, ProxyCommand
from scp import SCPClient

from src.config import QUERIES_DIR_RAW, DATA_DIR, DATABASE_PATH, TMP_DIR, logger, QUERIES_DIR_FROM_CLUSTER


class RemoteProcessor:
    def __init__(self, user_name: str, hostname: str, remote_data_path: str, remote_queries_dir: str):
        self.username = user_name
        self.hostname = hostname
        self.remote_data_path = remote_data_path
        self.remote_queries_dir = remote_queries_dir


DIAMONDS_4_PROCESSOR = RemoteProcessor(
    user_name='gross',
    hostname='diamonds4',
    remote_data_path='/export/scratch2/home/gross/SqlPile/data/',
    remote_queries_dir='/export/scratch2/home/gross/SqlPile/data/queries_v4'
)

CLUSTER_DATA_PATH = '/export/scratch2/home/gross/SqlPile/data/'

queries_dir_from_data_path = QUERIES_DIR_RAW.replace(DATA_DIR + '/', '')
CLUSTER_QUERIES_DIR = os.path.join(CLUSTER_DATA_PATH, queries_dir_from_data_path)

CLUSTER_UNIFIED_FILE_PATH = os.path.join(CLUSTER_QUERIES_DIR, 'parquet_queries_tmp.parquet')

def format_size(size: int) -> str:
    """Format size in bytes to a human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

def progress(filename, size, sent):
    percent = sent / size * 100
    size_formatted = format_size(size)
    sent_formatted = format_size(sent)
    print(f"\rTransferring {filename}: {percent:.2f}% ({sent_formatted}/{size_formatted})", end='')


def download_file():
    ssh = SSHClient()
    ssh.load_system_host_keys()  # or ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('example.com', username='your_user')

    with SCPClient(ssh.get_transport(), progress=progress) as scp:
        scp.get('/remote/path/your_big_file.is')


def get_existing_repos_parquet() -> str:
    con = duckdb.connect(DATABASE_PATH, read_only=True)

    parquet_path = os.path.join(TMP_DIR, 'repos.parquet')
    con.execute(f"COPY (SELECT DISTINCT repo_url FROM repos) TO '{parquet_path}' (FORMAT PARQUET, OVERWRITE TRUE)")
    con.close()

    count = duckdb.sql(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
    logger.info(f"Found {count} unique repo URLs and copied them to {parquet_path}")

    return parquet_path


def create_ssh_session(remote: RemoteProcessor) -> SSHClient:
    host_alias = remote.hostname  # e.g., "diamonds4"

    # Load SSH configuration
    ssh_config_file = os.path.expanduser("~/.ssh/config")
    with open(ssh_config_file) as f:
        ssh_config = SSHConfig()
        ssh_config.parse(f)

    host_config = ssh_config.lookup(host_alias)
    hostname = host_config.get("hostname", host_alias)
    username = host_config.get("user", os.getlogin())
    port = int(host_config.get("port", 22))
    identity_file = host_config.get("identityfile", [None])[0]

    # Prepare ProxyJump (if specified)
    proxy_command_str = host_config.get("proxycommand")
    if proxy_command_str is None and "proxyjump" in host_config:
        jump_host = host_config["proxyjump"]
        jump_config = ssh_config.lookup(jump_host)
        jump_user = jump_config.get("user", os.getlogin())
        jump_host_real = jump_config.get("hostname", jump_host)
        jump_identity = jump_config.get("identityfile", [None])[0]

        # Create equivalent ProxyCommand manually
        proxy_command_str = (
            f"ssh -W {hostname}:{port} "
            f"{jump_user}@{jump_host_real} "
            f"-i {jump_identity}"
        )

    # Set up the SSH client
    ssh = SSHClient()
    ssh.set_missing_host_key_policy(AutoAddPolicy())

    if proxy_command_str:
        proxy = ProxyCommand(proxy_command_str)
        ssh.connect(
            hostname=hostname,
            port=port,
            username=username,
            key_filename=identity_file,
            sock=proxy
        )
    else:
        ssh.connect(
            hostname=hostname,
            port=port,
            username=username,
            key_filename=identity_file
        )

    logger.info(f"Connected to {hostname} as {username}")
    return ssh


def get_cli_command_for_sql(sql: str, print: bool = False) -> str:
    """
    Generate a command to execute a SQL query using DuckDB in Python.
    """
    if print:
        return f'python3 -c "import duckdb; print(duckdb.sql(\\"{sql}\\").fetchall())"'
    return f'python3 -c "import duckdb; duckdb.sql(\\"{sql}\\")"'


def create_unified_parquet_on_remote(remote: RemoteProcessor) -> str:
    """
    will return the path to the unified parquet file on the remote server
    """

    ssh = create_ssh_session(remote)
    remote_tmp_parquet_path = os.path.join(remote.remote_data_path, 'unified_data.parquet')

    # execute the following command on the remote server
    copy_query = f"COPY (SELECT * FROM '{remote.remote_queries_dir}/*/*.parquet') TO '{remote_tmp_parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE TRUE)"
    command = get_cli_command_for_sql(copy_query)

    logger.info(f"Starting to create unified parquet file on remote server: {remote.hostname}. This may take a while...")
    stdin, stdout, stderr = ssh.exec_command(command)
    exit_status = stdout.channel.recv_exit_status()  # Wait for command to finish
    if exit_status == 0:
        logger.info(f"Unified parquet file created successfully at {remote_tmp_parquet_path}")
    else:
        logger.error(f"Error creating unified parquet file: {stderr.read().decode()}")

    # get the number of repos in the unified parquet file
    logger.info("Counting unique repo URLs in the unified parquet file...")
    count_query = f"SELECT COUNT(DISTINCT repo_url) FROM '{remote_tmp_parquet_path}'"
    command = get_cli_command_for_sql(count_query, print=True)
    stdin, stdout, stderr = ssh.exec_command(command)
    exit_status = stdout.channel.recv_exit_status()

    if exit_status == 0:
        unique_repo_count = stdout.read().decode().strip()
        logger.info(f"Counted {unique_repo_count} unique repo URLs in the unified parquet file at {remote_tmp_parquet_path}")
    else:
        logger.error(f"Error counting unique repo URLs: {stderr.read().decode()}")

    ssh.close()

    return remote_tmp_parquet_path


def download_unified_parquet(remote: RemoteProcessor, remote_file_path: str):

    local_file_path = os.path.join(QUERIES_DIR_FROM_CLUSTER, f'queries_{remote.hostname}.parquet')
    ssh = create_ssh_session(remote)
    with SCPClient(ssh.get_transport(), progress=progress) as scp:
        logger.info(f"Downloading unified parquet file from {remote_file_path} to {local_file_path}")
        scp.get(remote_file_path, local_file_path)

    logger.info(f"Unified parquet file downloaded successfully to {local_file_path}")


def download_data_from_remote():
    remote = DIAMONDS_4_PROCESSOR
    unified_path = create_unified_parquet_on_remote(remote)
    # unified_path = '/export/scratch2/home/gross/SqlPile/data/unified_data.parquet'
    download_unified_parquet(remote, unified_path)


if __name__ == "__main__":
    download_data_from_remote()
