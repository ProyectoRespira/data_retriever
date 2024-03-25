from src.mirror import start_mirror_process
from src.initialize_db import create_missing_tables

def main():
    create_missing_tables()
    start_mirror_process()

if __name__ == "__main__":
    main()