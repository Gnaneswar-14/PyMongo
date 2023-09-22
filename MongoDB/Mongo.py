# Import the libraries 

import pymongo
import warnings
import sys

# Filter warnings
warnings.filterwarnings('ignore')


class MongoDBManager:

    '''
    This class will contains the functions that will
    connect with mongo db and perform operations
    '''

    def __init__(self) -> None:

        '''
        Default constructor
        '''

        self.host = "localhost"
        self.port = 27017


    def init_connection(self) -> pymongo.MongoClient:

        '''
        Function used to connect with Mongo DB
        '''

        try:
            self.client = pymongo.MongoClient(f"mongodb://{self.host}:{self.port}/")
            print(f"Successfully connected to MongoDB.")

        except pymongo.errors.ConnectionFailure as e:
            print(f"Connection to MongoDB failed: {e}")
        
        except pymongo.errors.InvalidURI as e:
            print(f"Invalid MongoDB URI: {e}")
        
        except pymongo.errors.ConfigurationError as e:
            print(f"MongoDB configuration error: {e}")
        
        return self.client


    def create_db(self, db_name: str) -> None:

        '''
        Function used to create a DB in Mongo DB
        Param: db_name 
        '''

        self.db_name = db_name
        
        try:
            self.db = self.client[self.db_name]
            print(f'Successfully created {self.db_name} DB.')
        
        except pymongo.errors.ConnectionFailure as e:
            print(f'Connection to MongoDB failed: {e}')
        
        except pymongo.errors.DuplicateKeyError as e:
            print(f'Database {self.db_name} already exists.')
            sys.exit(1)
        
        except pymongo.errors.OperationFailure as e:
            print(f'Failed to create {self.db_name} DB: {e}')
        
        except Exception as e:
            print(f'An unexpected error occurred: {e}')


    def create_collection(self, collection_name: str) -> None:

        '''
        Function to create a collection in Mongo DB under a DB
        Params: db_name, collection_name
        '''

        self.collection_name = collection_name
        
        try:
            self.collection = self.db[self.collection_name]
            print(f'Successfully created {self.collection_name} collection.')
        
        except pymongo.errors.CollectionInvalid as e:
            print(f'Invalid collection name: {e}')
        
        except pymongo.errors.OperationFailure as e:
            print(f'Failed to create {self.collection_name} collection: {e}')
        
        except pymongo.errors.WriteError as e:
            print(f'Error writing data when creating {self.collection_name} collection: {e}')
        
        except Exception as e:
            print(f'An unexpected error occurred: {e}')


    def insert_one_record(self, data : dict) -> None:

        '''
        Function used to insert a single record into Mongo DB collection
        Params: data 
        '''

        self.data = data

        try:
            result = self.collection.insert_one(data)
            print(f'Successfully inserted record with ID: {result.inserted_id}')
        
        except pymongo.errors.WriteError as e:
            print(f'Error writing data when inserting a record: {e}')
        
        except Exception as e:
            print(f'An unexpected error occurred: {e}')


    def insert_many_records(self, data_list : list(dict)) -> None:
        
        '''
        Function used to insert multiple records at once into Mongo DB collection
        Params: data_list  
        '''

        self.data_list = data_list
        
        try:
            result = self.collection.insert_many(data_list)
            print(f'Successfully inserted {len(result.inserted_ids)} records.')
        
        except pymongo.errors.WriteError as e:
            print(f'Error writing data when inserting records: {e}')
        
        except Exception as e:
            print(f'An unexpected error occurred: {e}')


    def delete_one_record(self, query : dict) -> None:

        '''
        Function used to delete a single record from Mongo DB collection
        Params: query
        '''
        
        self.query = query

        try:
            result = self.collection.delete_one(query)
            
            if result.deleted_count == 1:
                print('Successfully deleted one record.')
            
            else:
                print('No matching records found to delete.')
        
        except Exception as e:
            print(f'An unexpected error occurred: {e}')


    def delete_many_records(self, query : list(dict)) -> None:

        '''
        Function used to delete multiple records from Mongo DB collection
        Params: query
        '''

        self.query = query 

        try:
            result = self.collection.delete_many(query)
            print(f'Successfully deleted {result.deleted_count} records.')
        
        except Exception as e:
            print(f'An unexpected error occurred: {e}')


    def update_one_record(self, query : dict, update_data : dict) -> None:

        '''
        Function used to update a single record in Mongo DB collection
        Params: query, update_data
        '''

        self.query = query
        self.update_data = update_data

        try:
            result = self.collection.update_one(query, {"$set": update_data})
            
            if result.matched_count == 1:
                print('Successfully updated one record.')
            
            else:
                print('No matching records found to update.')
        
        except Exception as e:
            print(f'An unexpected error occurred: {e}')


    def update_many_records(self, query : dict(dict), update_data : dict) -> None:

        '''
        Function used to update multiple records in Mongo DB collection
        Params: query, update_data
        '''

        try:
            result = self.collection.update_many(query, {"$set": update_data})
            print(f'Successfully updated {result.matched_count} records.')
       
        except Exception as e:
            print(f'An unexpected error occurred: {e}')


    def list_db(self) -> list:

        '''
        Function to list the available DBs present in
        Mongo DB
        '''

        for db in self.client.list_database_names():
            print(f'DB Name: {db}')


    def delete_collection(self, collection_name : str) -> None:

        '''
        Function used to delete a collection from Mongo DB
        '''

        self.collection_name = collection_name

        try:
            self.db.drop_collection(collection_name)
            print(f'Successfully deleted collection: {collection_name}')
        
        except pymongo.errors.CollectionInvalid as e:
            print(f'Invalid collection name: {e}')
        
        except Exception as e:
            print(f'An unexpected error occurred: {e}')


    def delete_db(self, db_name : str) -> None:

        '''
        Function used to delete a DB from Mongo DB
        Params: db_name
        '''

        self.db_name = db_name

        try:
            self.client.drop_database(db_name)
            print(f'Successfully deleted database: {db_name}')
        
        except pymongo.errors.OperationFailure as e:
            print(f'Failed to delete database: {e}')
        
        except Exception as e:
            print(f'An unexpected error occurred: {e}')


    def close_connection(self) -> None:

        '''
        Function to terminate the connection with Mongo DB
        '''

        self.client.close()
        print('Successfully terminated the connection with Mongo DB.')


# Example usage:

if __name__ == "__main__":

    obj = MongoDBManager()
    obj.init_connection()
    obj.create_db(db_name='Students')
    obj.create_collection(collection_name='Class_of_83')

    # Example: Insert One Record
    data = {"name": "John", "age": 25}
    obj.insert_one_record(data)

    # Example: Insert Many Records
    data_list = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 28}]
    obj.insert_many_records(data_list)

    # Example: Delete One Record
    delete_query = {"name": "John"}
    obj.delete_one_record(delete_query)

    # Example: Delete Many Records
    delete_query_many = {"age": {"$lt": 30}}
    obj.delete_many_records(delete_query_many)

    # Example: Update One Record
    update_query = {"name": "Alice"}
    update_data = {"age": 31}
    obj.update_one_record(update_query, update_data)

    # Example: Update Many Records
    update_query_many = {"age": {"$lt": 30}}
    update_data_many = {"status": "Young"}
    obj.update_many_records(update_query_many, update_data_many)

    # Example: Delete Collection
    obj.delete_collection(collection_name='Class_of_83')

    # Example: Delete Database
    obj.delete_db(db_name='Students')

    obj.close_connection()