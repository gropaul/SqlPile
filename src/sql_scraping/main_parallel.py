import os
import subprocess

def main(n_partitions: int):

    print(f"Starting SQL scraping with {n_partitions} partitions in parallel...")
    for part_idx in range(n_partitions):
        cmd = [
            "nohup",
            "poetry", "run", "python3",
            "src/sql_scraping/main.py",
            "-p", "1",
            "-t", "1",
            "--partition", str(part_idx),
            str(n_partitions),
        ]

        print(f"Starting partition {part_idx+1}/{n_partitions}: {' '.join(cmd)}")

        subprocess.Popen(
            cmd,
            stdout=open(f"partition_{part_idx}.out", "w"),
            stderr=open(f"partition_{part_idx}.err", "w"),
            preexec_fn=os.setpgrp  # fully detach from terminal
        )


    print(f"Started {n_partitions} partitions in parallel.")
    return

if __name__ == "__main__":
    # get number of partitions from terminal input
    n_partitions = int(input("Enter number of partitions to run in parallel: "))
    main(n_partitions)
