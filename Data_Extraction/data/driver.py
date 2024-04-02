from extract import extract
from transform import transform
from load import setup_database, load, verify_database

def main():
    data = extract()
    transformed = transform(data)
    setup_database()
    load(transformed)
    verify_database()

main()
