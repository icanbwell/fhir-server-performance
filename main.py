import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta

from logging import Logger
from typing import Any, List, Dict, Optional, Union

import aiofiles
from helix_fhir_client_sdk.fhir_client import FhirClient

from helix_fhir_client_sdk.loggers.fhir_logger import FhirLogger

from dotenv import load_dotenv


class MyLogger(FhirLogger):
    def __init__(self):
        self._internal_logger: Logger = logging.getLogger("FhirPerformance")
        self._internal_logger.setLevel(logging.DEBUG)

    def info(self, param: Any) -> None:
        """
        Handle messages at INFO level
        """
        # self._internal_logger.info(param)
        pass

    def error(self, param: Any) -> None:
        """
        Handle messages at error level
        """
        self._internal_logger.error(param)


class ResourceDownloader:
    def __init__(self) -> None:
        fhir_server = "fhir.icanbwell.com"
        # fhir_server = "fhir-next.icanbwell.com"
        self.server_url = f"https://{fhir_server}/4_0_0"
        assert os.environ.get("FHIR_CLIENT_ID"), "FHIR_CLIENT_ID environment variable must be set"
        assert os.environ.get("FHIR_CLIENT_SECRET"), "FHIR_CLIENT_SECRET environment variable must be set"
        self.auth_client_id = os.environ.get("FHIR_CLIENT_ID")
        self.auth_client_secret = os.environ.get("FHIR_CLIENT_SECRET")
        self.resource = "AuditEvent"
        assert os.environ.get("FHIR_CLIENT_TAG"), "FHIR_CLIENT_TAG environment variable must be set"
        self.client = os.environ.get("FHIR_CLIENT_TAG")
        self.auth_scopes = [f"user/{self.resource}.read", f"access/{self.client}.*"]
        self.page_size_for_retrieving_ids = 10000
        self.start_date = datetime.strptime("2021-12-31", "%Y-%m-%d")
        self.end_date = datetime.strptime("2022-01-01", "%Y-%m-%d")
        assert self.end_date > self.start_date
        self.concurrent_requests = 10
        self.page_size_for_retrieving_resources = 100
        self.use_data_streaming: bool = False
        self.use_atlas: bool = False

    async def load_data(self, name):
        start_job = time.time()

        output_file_streaming_ids = await aiofiles.open('output_ids.json', mode='wb')
        output_file_streaming_resources = await aiofiles.open('output_resources.json', mode='wb')
        output_file = await aiofiles.open('output.json', mode='w')

        resource_count_holder = {
            "resource_count": 0,
            "total_bytes": 0,
            "startTime": 0.0
        }

        id_count_holder = {
            "resource_count": 0,
            "total_bytes": 0,
            "startTime": 0.0
        }

        streaming_id_count_holder = {
            "resource_count": 0,
            "total_bytes": 0,
            "startTime": 0.0
        }

        streaming_chunk_count_holder = {
            "resource_count": 0,
            "total_bytes": 0,
            "startTime": 0.0
        }

        resources: List[Dict[str, Any]] = []

        async def on_received_data(id_count_holder1: Dict[str, Union[int, float]],
                                   resource_count_holder1: Dict[str, Union[int, float]], data: List[Dict[str, Any]],
                                   batch_number: Optional[int]) -> bool:
            # print(f"received batch: {batch_number}")
            number_of_ids_to_expect: int = id_count_holder1["resource_count"]
            if resource_count_holder1["startTime"] == 0.0:
                print("\n")
                resource_count_holder1["startTime"] = time.time()
            for resource in data:
                resources.append(resource)
            chunk_end_time = time.time()
            json_result: str = json.dumps(data)
            resource_count_holder1["resource_count"] = resource_count_holder1["resource_count"] + len(data)
            resource_count_holder1["total_bytes"] = resource_count_holder1["total_bytes"] + len(
                json_result.encode("utf-8"))
            time_difference = timedelta(seconds=chunk_end_time - resource_count_holder1["startTime"])
            kilo_bytes_per_sec = resource_count_holder1["total_bytes"] / (
                    time_difference.total_seconds() * 1024) if time_difference.total_seconds() > 0.0 else 0
            total_megabytes = resource_count_holder1["total_bytes"] / (1024 * 1024)
            resource_count = resource_count_holder1['resource_count']
            resource_count_per_sec = resource_count / time_difference.total_seconds() if time_difference.total_seconds() > 0.0 else 0
            remaining_resource_count = number_of_ids_to_expect - resource_count
            estimate_remaining_time = (remaining_resource_count / resource_count_per_sec)
            print(f"Resources: [{resource_count :,} / {number_of_ids_to_expect:,}] {time_difference},"
                  + f" Resource/sec={resource_count_per_sec:.2f}"
                  + f" Total MB={total_megabytes:.0f} KB/sec={kilo_bytes_per_sec:.2f}"
                  + f" Remaining={timedelta(seconds=estimate_remaining_time)}",
                  end='\r')
            await output_file.write(json_result)
            await output_file.flush()
            return True

        async def on_error(error: str, resources1: str, page_number: Optional[int]) -> bool:
            print(f"=== ERROR: {error} ===")
            return True

        async def on_received_ids(id_count_holder1: Dict[str, Union[int, float]], data: List[Dict[str, Any]],
                                  batch_number: Optional[int]) -> bool:
            if id_count_holder1["startTime"] == 0.0:
                print("\n")
                id_count_holder1["startTime"] = time.time()
            chunk_end_time = time.time()
            json_result: str = json.dumps(data)
            id_count_holder1["resource_count"] = id_count_holder1["resource_count"] + len(data)
            id_count_holder1["total_bytes"] = id_count_holder1["total_bytes"] + len(
                json_result.encode("utf-8"))
            time_difference = timedelta(seconds=chunk_end_time - id_count_holder1["startTime"])
            kilo_bytes_per_sec = id_count_holder1["total_bytes"] / (
                    time_difference.total_seconds() * 1024) if time_difference.total_seconds() > 0.0 else 0
            total_megabytes = id_count_holder1["total_bytes"] / (1024 * 1024)
            resource_count = id_count_holder1['resource_count']
            resource_count_per_sec = resource_count / time_difference.total_seconds() if time_difference.total_seconds() > 0.0 else 0
            print(f"Ids: [{resource_count:,}] {time_difference},"
                  + f" Resource/sec={resource_count_per_sec:.2f}"
                  + f" Total MB={total_megabytes:.0f} KB/sec={kilo_bytes_per_sec:.2f}",
                  end='\r')
            return True

        async def on_received_streaming_ids(resource_count_holder1: Dict[str, Union[int, float]],
                                            data: bytes, page_number: Optional[int]) -> bool:
            if resource_count_holder1["startTime"] == 0.0:
                print("\n")
                resource_count_holder1["startTime"] = time.time()
            chunk_end_time = time.time()
            resource_count_holder1["resource_count"] += 1
            resource_count_holder1["total_bytes"] = resource_count_holder1["total_bytes"] + len(data)
            time_difference = timedelta(seconds=chunk_end_time - resource_count_holder1["startTime"])
            kilo_bytes_per_sec = resource_count_holder1["total_bytes"] / (
                    time_difference.total_seconds() * 1024) if time_difference.total_seconds() > 0.0 else 0
            total_megabytes = resource_count_holder1["total_bytes"] / (1024 * 1024)
            print(f"Streaming ids: [{resource_count_holder1['resource_count']:,}] {time_difference},"
                  + f" Total MB={total_megabytes:.0f} KB/sec={kilo_bytes_per_sec:.2f}",
                  end='\r')
            await output_file_streaming_ids.write(data)
            # await output_file.flush()
            return True

        async def on_received_streaming_chunk(resource_count_holder1: Dict[str, Union[int, float]],
                                              data: bytes, page_number: Optional[int]) -> bool:
            if resource_count_holder1["startTime"] == 0.0:
                print("\n")
                resource_count_holder1["startTime"] = time.time()
            chunk_end_time = time.time()
            resource_count_holder1["resource_count"] += 1
            resource_count_holder1["total_bytes"] = resource_count_holder1["total_bytes"] + len(data)
            time_difference = timedelta(seconds=chunk_end_time - resource_count_holder1["startTime"])
            kilo_bytes_per_sec = resource_count_holder1["total_bytes"] / (
                    time_difference.total_seconds() * 1024) if time_difference.total_seconds() > 0.0 else 0
            total_megabytes = resource_count_holder1["total_bytes"] / (1024 * 1024)
            print(f"Streaming chunks: [{resource_count_holder1['resource_count']:,}] {time_difference},"
                  + f" Total MB={total_megabytes:.0f} KB/sec={kilo_bytes_per_sec:.2f}",
                  end='\r')
            await output_file_streaming_resources.write(data)
            # await output_file.flush()
            return True

        # Use a breakpoint in the code line below to debug your script.
        print(f'Calling {self.server_url} with {self.concurrent_requests} parallel connections...')
        # from helix_fhir_client_sdk.fhir_client import FhirClient
        fhir_client = await self.create_fhir_client()
        await fhir_client.get_resources_by_query_and_last_updated_async(
            concurrent_requests=self.concurrent_requests,
            page_size_for_retrieving_resources=self.page_size_for_retrieving_resources,
            page_size_for_retrieving_ids=self.page_size_for_retrieving_ids,
            last_updated_start_date=self.start_date,
            last_updated_end_date=self.end_date,
            fn_handle_batch=lambda data, batch_number: on_received_data(id_count_holder, resource_count_holder, data,
                                                                        batch_number),
            fn_handle_error=on_error,
            fn_handle_ids=lambda data, batch_number: on_received_ids(id_count_holder, data, batch_number),
            fn_handle_streaming_ids=lambda data, batch_number: on_received_streaming_ids(streaming_id_count_holder,
                                                                                         data, batch_number),
            fn_handle_streaming_chunk=lambda data, batch_number: on_received_streaming_chunk(
                streaming_chunk_count_holder,
                data, batch_number)
        )

        end_job = time.time()
        await output_file_streaming_ids.flush()
        await output_file_streaming_ids.close()
        await output_file_streaming_resources.flush()
        await output_file_streaming_resources.close()
        await output_file.flush()
        await output_file.close()
        print(f"\n====== Received {len(resources)} resources in {timedelta(seconds=end_job - start_job)} =======")

        # for id_ in list_of_ids:
        #     print(id_)

    async def create_fhir_client(self):
        fhir_client: FhirClient = FhirClient()
        fhir_client = fhir_client.url(self.server_url)
        fhir_client = fhir_client.client_credentials(self.auth_client_id, self.auth_client_secret)
        fhir_client = fhir_client.auth_scopes(self.auth_scopes)
        fhir_client = fhir_client.resource(self.resource)
        fhir_client = fhir_client.logger(MyLogger())
        if self.use_atlas:
            fhir_client = fhir_client.additional_parameters(["_useAtlas=1"])
        if self.use_data_streaming:
            fhir_client = fhir_client.additional_parameters(["_streamResponse=1"])
            fhir_client = fhir_client.use_data_streaming(True)
        return fhir_client


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    load_dotenv()

    asyncio.run(ResourceDownloader().load_data('PyCharm'))
