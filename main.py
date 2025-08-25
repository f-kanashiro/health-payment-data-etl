from scripts import split_raw_csv
from scripts import transform_data
from scripts import load_to_db

def main():
    split_raw_csv.run()
    #transform_data.run()
    #load_to_db.run()

if __name__ == "__main__":
    main()