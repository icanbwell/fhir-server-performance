# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Generator
import time
import asyncio
from helix_fhir_client_sdk.filters.base_filter import BaseFilter

from async_fhir_client import AsyncFhirClient


class LastUpdatedFilter(BaseFilter):
    def __init__(self, less_than: Optional[datetime], greater_than: Optional[datetime]) -> None:
        """
        Returns resources between the date ranges


        :param less_than:
        :param greater_than:
        """
        assert (less_than is None or isinstance(less_than, datetime))
        self.less_than: Optional[datetime] = less_than
        assert (greater_than is None or isinstance(greater_than, datetime))
        self.greater_than: Optional[datetime] = greater_than

    def __str__(self) -> str:
        filters: List[str] = []
        if self.less_than is not None:
            filters.append(f"_lastUpdated=lt{self.less_than.strftime('%Y-%m-%d')}")
        if self.greater_than is not None:
            filters.append(f"_lastUpdated=gt{self.greater_than.strftime('%Y-%m-%d')}")
        return "&".join(filters)


# def split_array(array, number_of_chunks) -> Generator[List[str], None, None]:
#     """
#     Splits an array into chunks
#     :param array:
#     :param number_of_chunks:
#     """
#     k, m = divmod(len(array), number_of_chunks)
#     return (array[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(number_of_chunks))


# Yield successive n-sized chunks from l.
def divide_into_chunks(array: List[Any], chunk_size: int) -> Generator[List[str], None, None]:
    # looping till length l
    for i in range(0, len(array), chunk_size):
        yield array[i:i + chunk_size]


class ResourceDownloader:
    def __init__(self) -> None:
        self.server_url = "https://fhir.icanbwell.com/4_0_0"
        self.auth_client_id = os.environ.get("FHIR_CLIENT_ID")
        assert self.auth_client_id
        self.auth_client_secret = os.environ.get("FHIR_CLIENT_SECRET")
        assert self.auth_client_secret
        self.resource = "AuditEvent"
        self.client = "medstar"
        self.page_size = 10000
        self.auth_scopes = [f"user/{self.resource}.read", f"access/{self.client}.*"]
        self.start_date = datetime.strptime("2022-01-01", "%Y-%m-%d")
        self.end_date = datetime.strptime("2022-01-02", "%Y-%m-%d")

    @staticmethod
    def handle_error(error, response, page_number):
        print(f"{error}: {response}")
        return True

    async def print_hi(self, name):
        start_job = time.time()

        # Use a breakpoint in the code line below to debug your script.
        print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
        # from helix_fhir_client_sdk.fhir_client import FhirClient
        fhir_client = await self.create_fhir_client()
        fhir_client = fhir_client.include_only_properties(["id"])
        fhir_client = fhir_client.page_size(self.page_size)

        # loop by dates
        # set up initial filter
        greater_than = self.start_date - timedelta(days=1)
        less_than = greater_than + timedelta(days=1)
        last_updated_filter = LastUpdatedFilter(less_than=less_than, greater_than=greater_than)
        fhir_client = fhir_client.filter(
            [
                last_updated_filter
            ]
        )
        list_of_ids: List[str] = []

        def add_to_list(resources_: List[Dict[str, Any]], page_number) -> bool:
            end_batch = time.time()
            list_of_ids.extend([resource_['id'] for resource_ in resources_])
            print(
                f"Received {len(resources_)} ids from page {page_number} (total={len(list_of_ids)}) in {(end_batch - start)}"
                f" starting with id: {resources_[0]['id'] if len(resources_) > 0 else 'none'}")

            return True

        concurrent_requests: int = 10

        # get token first
        await fhir_client.access_token

        while greater_than < self.end_date:
            greater_than = greater_than + timedelta(days=1)
            less_than = greater_than + timedelta(days=1)
            print(f"===== Processing date {greater_than} =======")
            last_updated_filter.less_than = less_than
            last_updated_filter.greater_than = greater_than
            start = time.time()
            await fhir_client.get_by_query_in_pages(
                concurrent_requests=concurrent_requests,
                fn_handle_batch=lambda resp, page_number: add_to_list(resp, page_number),
                fn_handle_error=self.handle_error
            )
            end = time.time()
            print(f"Runtime processing date is {end - start} for {len(list_of_ids)} ids")

        print(f"====== Received {len(list_of_ids)} ids =======")
        # now split the ids
        chunk_size: int = 100
        chunks: Generator[List[str], None, None] = divide_into_chunks(list_of_ids, chunk_size)

        # chunks_list = list(chunks)

        resources = []

        def add_resources_to_list(resources_: List[Dict[str, Any]], page_number) -> bool:
            end_batch = time.time()
            resources.extend([resource_ for resource_ in resources_])
            print(
                f"Received {len(resources_)} resources in {(end_batch - start)} page={page_number}"
                f" starting with resource: {resources_[0]['id'] if len(resources_) > 0 else 'none'}"
            )

            return True

        # create a new one to reset all the properties
        fhir_client = await self.create_fhir_client()
        await fhir_client.get_resources_by_id_in_parallel_batches(
            concurrent_requests=concurrent_requests,
            chunks=chunks,
            fn_handle_batch=lambda resp, page_number: add_resources_to_list(resp, page_number),
            fn_handle_error=self.handle_error
        )

        end_job = time.time()
        print(f"====== Received {len(resources)} resources in {(end_job - start_job)} =======")

        # for id_ in list_of_ids:
        #     print(id_)

    async def create_fhir_client(self):
        fhir_client: AsyncFhirClient = AsyncFhirClient()
        fhir_client = fhir_client.url(self.server_url)
        fhir_client = fhir_client.client_credentials(self.auth_client_id, self.auth_client_secret)
        fhir_client = fhir_client.auth_scopes(self.auth_scopes)
        fhir_client = fhir_client.resource(self.resource)
        return fhir_client


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    asyncio.run(ResourceDownloader().print_hi('PyCharm'))
