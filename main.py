import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta

from logging import Logger
from typing import Any, List, Dict, Optional

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

    async def load_data(self, name):
        start_job = time.time()

        output_file = await aiofiles.open('output.json', mode='w')

        resource_count_holder = {
            "resource_count": 0
        }

        async def on_received_data(resource_count_holder1: Dict[str, int], data: List[Dict[str, Any]],
                                   batch_number: Optional[int]) -> bool:
            # print(f"received batch: {batch_number}")
            chunk_end_time = time.time()
            resource_count_holder1["resource_count"] = resource_count_holder1["resource_count"]  + len(data)
            print(f"[{resource_count_holder1['resource_count']}] {timedelta(seconds=chunk_end_time - start_job)}", end='\r')
            await output_file.write(json.dumps(data))
            await output_file.flush()
            return True

        # Use a breakpoint in the code line below to debug your script.
        print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
        # from helix_fhir_client_sdk.fhir_client import FhirClient
        fhir_client = await self.create_fhir_client()
        resources = await fhir_client.get_resources_by_query_and_last_updated_async(
            concurrent_requests=self.concurrent_requests,
            page_size_for_retrieving_resources=self.page_size_for_retrieving_resources,
            page_size_for_retrieving_ids=self.page_size_for_retrieving_ids,
            last_updated_start_date=self.start_date,
            last_updated_end_date=self.end_date,
            fn_handle_batch=lambda data, batch_number: on_received_data(resource_count_holder, data, batch_number)
        )

        end_job = time.time()
        await output_file.close()
        print(f"====== Received {len(resources)} resources in {timedelta(seconds=end_job - start_job)} =======")

        # for id_ in list_of_ids:
        #     print(id_)

    async def create_fhir_client(self):
        fhir_client: FhirClient = FhirClient()
        fhir_client = fhir_client.url(self.server_url)
        fhir_client = fhir_client.client_credentials(self.auth_client_id, self.auth_client_secret)
        fhir_client = fhir_client.auth_scopes(self.auth_scopes)
        fhir_client = fhir_client.resource(self.resource)
        fhir_client = fhir_client.logger(MyLogger())
        # fhir_client = fhir_client.additional_parameters(["_useAtlas=1"])
        return fhir_client


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    load_dotenv()

    asyncio.run(ResourceDownloader().load_data('PyCharm'))
