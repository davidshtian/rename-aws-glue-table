import boto3
import argparse
import sys


def rename_glue_table(client, database_name, old_table_name, new_table_name):

    # Get old table information
    try:
        table_response = client.get_table(
            DatabaseName=database_name, Name=old_table_name)
    except Exception as e:
        print(e)
        sys.exit()

    # Edit new table name
    table_input = table_response["Table"]
    table_input["Name"] = new_table_name

    # Pop keys which are needed creating table
    table_input.pop("CreatedBy")
    table_input.pop("CreateTime")
    table_input.pop("UpdateTime")
    table_input.pop("DatabaseName")
    table_input.pop("IsRegisteredWithLakeFormation")
    table_input.pop("CatalogId")

    # Create new table
    try:
        client.create_table(DatabaseName=database_name, TableInput=table_input)
    except Exception as e:
        print(e)
        sys.exit()

    # Get old table partitions
    try:
        partition_response = client.get_partitions(
            DatabaseName=database_name, TableName=old_table_name)
    except Exception as e:
        print(e)
        sys.exit()

    # Edit new partitions
    partition_input = partition_response["Partitions"]

    # Pop keys which are needed creating partitions
    for partition in partition_input:
        partition.pop("TableName")
        partition.pop("CreationTime")
        partition.pop("DatabaseName")
        partition.pop("CatalogId")

    # Create new partitions
    try:
        client.batch_create_partition(DatabaseName=database_name,
                                      TableName=new_table_name, PartitionInputList=partition_input)
    except Exception as e:
        print(e)
        sys.exit()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    # Parse args
    parser.add_argument('--database-name', type=str, default='default')
    parser.add_argument('--old-table-name', type=str, default='old_table')
    parser.add_argument('--new-table-name', type=str, default='new_table')
    parser.add_argument('--region-name', type=str, default='us-east-1')

    args, _ = parser.parse_known_args()

    # Get database, table and region info
    database_name = args.database_name
    old_table_name = args.old_table_name
    new_table_name = args.new_table_name
    region_name = args.region_name

    # Print database, table and region info
    print(f'# Database: {database_name}')
    print(f'# Old Table: {old_table_name}')
    print(f'# New Table: {new_table_name}')
    print(f'# Region: {region_name}')

    # Init boto3 client
    client = boto3.client("glue", region_name=args.region_name)

    # Call the main function
    rename_glue_table(client, database_name, old_table_name, new_table_name)

    # Completed. Yeah~
    print('# Completed.')
