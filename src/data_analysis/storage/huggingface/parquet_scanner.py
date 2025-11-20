#!/usr/bin/env python3
"""
Scan HuggingFace datasets for parquet files and store metadata in DuckDB.
"""

from huggingface_hub import HfApi, HfFileSystem
import duckdb
import re
from datetime import datetime


class HFParquetScanner:
    def __init__(self, db_path="hf_parquet_files.duckdb"):
        self.api = HfApi()
        self.fs = HfFileSystem()
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._create_tables()

    def _create_tables(self):
        """Create the database schema."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                dataset_id VARCHAR PRIMARY KEY,
                last_scanned TIMESTAMP,
                total_parquet_files INTEGER,
                total_size_bytes BIGINT
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS parquet_files (
                id INTEGER PRIMARY KEY,
                dataset_id VARCHAR,
                file_path VARCHAR,
                hf_url VARCHAR,
                size_bytes BIGINT,
                table_group VARCHAR,
                split_name VARCHAR,
                shard_index INTEGER,
                discovered_at TIMESTAMP,
                FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id)
            )
        """)

        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS parquet_file_id_seq START 1
        """)


    def _parse_file_path(self, file_path, dataset_id):
        """Extract table group, split name, and shard index from file path."""
        # Remove dataset prefix
        path = file_path.replace(f"datasets/{dataset_id}/", "")
        parts = path.split('/')
        filename = parts[-1].replace('.parquet', '')

        # Determine table group (directory structure)
        if len(parts) > 1:
            table_group = '/'.join(parts[:-1])
        else:
            table_group = 'root'

        # Try to extract split name and shard index
        # Common patterns: train-00000.parquet, train/data-00000.parquet
        split_name = None
        shard_index = None

        # Check directory name for split
        if len(parts) > 1:
            potential_split = parts[-2]
            if potential_split in ['train', 'test', 'validation', 'dev']:
                split_name = potential_split

        # Check filename for split and shard
        match = re.match(r'(.+?)-(\d+)', filename)
        if match:
            if not split_name:
                split_name = match.group(1)
            shard_index = int(match.group(2))
        elif not split_name:
            # Use filename as split if no pattern found
            split_name = filename

        return table_group, split_name, shard_index

    def scan_dataset(self, dataset_id):
        """Scan a single dataset for parquet files and store in DB."""
        print(f"Scanning {dataset_id}...")

        try:
            # List all files recursively
            files = self.fs.ls(f"datasets/{dataset_id}", detail=True, recursive=True)

            # Filter for parquet files
            parquet_files = [
                f for f in files
                if f['type'] == 'file' and f['name'].endswith('.parquet')
            ]

            if not parquet_files:
                print(f"  No parquet files found in {dataset_id}")
                return

            total_size = sum(f['size'] for f in parquet_files)
            discovered_at = datetime.now()

            # Insert/update dataset record
            self.conn.execute("""
                INSERT OR REPLACE INTO datasets (dataset_id, last_scanned, total_parquet_files, total_size_bytes)
                VALUES (?, ?, ?, ?)
            """, [dataset_id, discovered_at, len(parquet_files), total_size])

            # Insert parquet file records
            for file in parquet_files:
                file_path = file['name'].replace("datasets/", "")
                hf_url = f"hf://{file['name']}"
                table_group, split_name, shard_index = self._parse_file_path(file['name'], dataset_id)

                self.conn.execute("""
                    INSERT INTO parquet_files 
                    (id, dataset_id, file_path, hf_url, size_bytes, table_group, split_name, shard_index, discovered_at)
                    VALUES (nextval('parquet_file_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?)
                """, [dataset_id, file_path, hf_url, file['size'], table_group, split_name, shard_index, discovered_at])

            print(f"  Found {len(parquet_files)} parquet files ({total_size / 1024 / 1024:.2f} MB)")

        except Exception as e:
            print(f"  Error scanning {dataset_id}: {e}")

    def scan_multiple_datasets(self, dataset_ids):
        """Scan multiple datasets."""
        for dataset_id in dataset_ids:
            self.scan_dataset(dataset_id)

    def search_and_scan_datasets(self, limit=100, filter_keyword=None):
        """Search for datasets and scan them for parquet files."""
        print(f"Searching for up to {limit} datasets...")

        datasets = self.api.list_datasets(
            limit=limit,

            sort="downloads",
            direction=-1
        )

        scanned = 0
        for dataset in datasets:
            dataset_id = dataset.id

            # Optional filtering
            if filter_keyword and filter_keyword.lower() not in dataset_id.lower():
                continue

            self.scan_dataset(dataset_id)
            scanned += 1

        print(f"\nScanned {scanned} datasets")

    def get_dataset_summary(self):
        """Get summary statistics."""
        return self.conn.execute("""
            SELECT 
                COUNT(*) as total_datasets,
                SUM(total_parquet_files) as total_files,
                SUM(total_size_bytes) / 1024 / 1024 / 1024 as total_size_gb
            FROM datasets
        """).fetchdf()

    def get_files_by_table_group(self, dataset_id=None):
        """Get parquet files grouped by table."""
        if dataset_id:
            return self.conn.execute("""
                SELECT 
                    dataset_id,
                    table_group,
                    split_name,
                    COUNT(*) as num_files,
                    SUM(size_bytes) / 1024 / 1024 as total_size_mb,
                    LIST(hf_url ORDER BY shard_index) as file_urls
                FROM parquet_files
                WHERE dataset_id = ?
                GROUP BY dataset_id, table_group, split_name
                ORDER BY table_group, split_name
            """, [dataset_id]).fetchdf()
        else:
            return self.conn.execute("""
                SELECT 
                    dataset_id,
                    table_group,
                    split_name,
                    COUNT(*) as num_files,
                    SUM(size_bytes) / 1024 / 1024 as total_size_mb
                FROM parquet_files
                GROUP BY dataset_id, table_group, split_name
                ORDER BY dataset_id, table_group, split_name
            """).fetchdf()

    def get_all_urls_for_table(self, dataset_id, table_group, split_name=None):
        """Get all parquet URLs for a specific table."""
        if split_name:
            return self.conn.execute("""
                SELECT hf_url
                FROM parquet_files
                WHERE dataset_id = ? AND table_group = ? AND split_name = ?
                ORDER BY shard_index
            """, [dataset_id, table_group, split_name]).fetchdf()
        else:
            return self.conn.execute("""
                SELECT hf_url
                FROM parquet_files
                WHERE dataset_id = ? AND table_group = ?
                ORDER BY split_name, shard_index
            """, [dataset_id, table_group]).fetchdf()

    def close(self):
        """Close the database connection."""
        self.conn.close()


def main():
    # Example usage
    scanner = HFParquetScanner("hf_parquet_files.duckdb")

    scanner.search_and_scan_datasets(limit=50)

    # Query the results
    print("\n=== Dataset Summary ===")
    print(scanner.get_dataset_summary())

    print("\n=== Files by Table Group ===")
    print(scanner.get_files_by_table_group("ibm/duorc"))

    print("\n=== Get URLs for specific table ===")
    urls = scanner.get_all_urls_for_table("ibm/duorc", "ParaphraseRC", "train")
    print(urls)

    scanner.close()


if __name__ == "__main__":
    main()