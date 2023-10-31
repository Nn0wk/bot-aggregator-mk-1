from datetime import datetime
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from dateutil.relativedelta import relativedelta  

from builld_pipeline import *
from dates import date_range
from data_processing import process_data


class DatabaseAggregator():
    def __init__(self, database_name, collection_name, url='localhost', port=27017, connection_args = {}, init=True, **kwargs):
        self.url = url 
        self.port = port
        self.database_name = database_name
        self.collection_name = collection_name 
        self.connection_args = connection_args
        self.max_result = None 

        if init:
            self.init()

        for key, value in kwargs.items():
            setattr(self, key, value)

    
    def init(self):
        print(f"Connecting to a DB at {self.url}:{self.port}")
        self.client = AsyncIOMotorClient(self.url, self.port, **self.connection_args)
        self.db = self.client[self.database_name]

    
    def _create_pipeline_by_group(self, iso_date_from, iso_date_to, group_by):
        pipeline = []
        if group_by == "month":
            pipeline = month_pipeline(iso_date_from, iso_date_to)
        elif group_by == "day":
            pipeline = day_pipeline(iso_date_from, iso_date_to)
        elif group_by == "hour":
            pipeline = hour_pipeline(iso_date_from, iso_date_to)
        return pipeline

    
    def _process_aggregation(self, results, iso_date_from, iso_date_to, group_by):
        results_processed = []
        dates_processed = []
        dates_generated = date_range(iso_date_from, iso_date_to, group_by) 
        for result in results:
            results_processed.append(result.get("total_value"))
            dates_processed.append(result.get("first").isoformat())
        return process_data(dates_generated, dates_processed, results_processed, group_by)


    async def aggregate(self, iso_date_from, iso_date_to, group_by):
        collection = self.db.get_collection(self.collection_name)
        pipeline = self._create_pipeline_by_group(iso_date_from, iso_date_to, group_by)
        cursor = collection.aggregate(pipeline=pipeline, allowDiskUse=True)
        results = await cursor.to_list(length=self.max_result)
        processed_results = self._process_aggregation(results, iso_date_from, iso_date_to, group_by)
        return processed_results 

    


   
