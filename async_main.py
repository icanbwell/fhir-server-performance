# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Generator, Callable
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


def split_array(array, number_of_chunks) -> Generator[List[str], None, None]:
    """
    Splits an array into chunks
    :param array:
    :param number_of_chunks:
    """
    k, m = divmod(len(array), number_of_chunks)
    return (array[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(number_of_chunks))


def get_chunk():
    pass


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

    async def print_hi(self, name):
        # Use a breakpoint in the code line below to debug your script.
        print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
        # from helix_fhir_client_sdk.fhir_client import FhirClient
        fhir_client = await self.create_fhir_client()
        fhir_client = fhir_client.include_only_properties(["id"])

        # loop by dates
        start_date = datetime.strptime("2022-01-01", "%Y-%m-%d")
        end_date = datetime.strptime("2022-01-02", "%Y-%m-%d")
        # set up initial filter
        greater_than = start_date - timedelta(days=1)
        less_than = greater_than + timedelta(days=1)
        last_updated_filter = LastUpdatedFilter(less_than=less_than, greater_than=greater_than)
        fhir_client = fhir_client.filter(
            [
                last_updated_filter
            ]
        )
        list_of_ids: List[str] = []

        def add_to_list(resources: List[Dict[str, Any]], page_number) -> bool:
            end_batch = time.time()
            list_of_ids.extend([resource_['id'] for resource_ in resources])
            print(
                f"Received {len(resources)} resources from page {page_number} (total={len(list_of_ids)}) in {(end_batch - start)}"
                f" starting with resource: {resources[0]['id'] if len(resources) > 0 else 'none'}")

            return True

        concurrent_requests: int = 10

        # get token first
        await fhir_client.access_token

        while greater_than < end_date:
            greater_than = greater_than + timedelta(days=1)
            less_than = greater_than + timedelta(days=1)
            print(f"===== Processing date {greater_than} =======")
            last_updated_filter.less_than = less_than
            last_updated_filter.greater_than = greater_than
            start = time.time()
            await fhir_client.get_in_batches(
                concurrent_requests=concurrent_requests,
                fn_handle_batch=lambda resp, page_number: add_to_list(resp, page_number))
            end = time.time()
            print(f"Runtime processing date is {end - start} for {len(list_of_ids)} records")

        # now split the ids
        chunks: Generator[List[str], None, None] = split_array(list_of_ids, concurrent_requests)

        resources = []

        def add_resources_to_list(resources_: List[Dict[str, Any]], page_number) -> bool:
            end_batch = time.time()
            resources.extend([resource_ for resource_ in resources_])
            print(f"Received {len(resources_)} resources in {(end_batch - start)}")
            return True

        await self.process_chunks(chunks=chunks,
                                  fn_handle_batch=lambda resp, page_number: add_resources_to_list(resp, page_number))

        # for id_ in list_of_ids:
        #     print(id_)

    async def create_fhir_client(self):
        fhir_client: AsyncFhirClient = AsyncFhirClient()
        fhir_client = fhir_client.url(self.server_url)
        fhir_client = fhir_client.client_credentials(self.auth_client_id, self.auth_client_secret)
        fhir_client = fhir_client.auth_scopes(self.auth_scopes)
        fhir_client = fhir_client.resource(self.resource)
        fhir_client = fhir_client.page_size(self.page_size)
        return fhir_client

    async def process_chunks(self, chunks: Generator[List[str], None, None],
                             fn_handle_batch: Optional[Callable[[List[Dict[str, Any]]], bool]]):
        async with AsyncFhirClient.create_http_session() as http:
            # noinspection PyTypeChecker
            result_list: List[List[Dict[str, Any]]] = await asyncio.gather(
                *(self.process(http, chunk, fn_handle_batch) for chunk in chunks)
            )
            return result_list

    async def process(self, session, chunk: List[str],
                      fn_handle_batch: Optional[Callable[[List[Dict[str, Any]]], bool]]) -> List[Dict[str, Any]]:
        fhir_client: AsyncFhirClient = await self.create_fhir_client()
        fhir_client.id_(chunk)
        result: List[Dict[str, Any]] = await fhir_client.get_with_handler(
            session=session,
            page_number=0,
            fn_handle_batch=fn_handle_batch
        )
        return result


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    asyncio.run(ResourceDownloader().print_hi('PyCharm'))
