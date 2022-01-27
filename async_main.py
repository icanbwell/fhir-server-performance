# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
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


async def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
    # from helix_fhir_client_sdk.fhir_client import FhirClient
    server_url = "https://fhir.icanbwell.com/4_0_0"
    auth_client_id = os.environ.get("FHIR_CLIENT_ID")
    assert auth_client_id
    auth_client_secret = os.environ.get("FHIR_CLIENT_SECRET")
    assert auth_client_secret
    resource = "AuditEvent"
    client = "medstar"
    page_size = 10000

    auth_scopes = [f"user/{resource}.read", f"access/{client}.*"]
    fhir_client: AsyncFhirClient = AsyncFhirClient()
    fhir_client = fhir_client.url(server_url)
    fhir_client = fhir_client.client_credentials(auth_client_id, auth_client_secret)
    fhir_client = fhir_client.auth_scopes(auth_scopes)
    fhir_client = fhir_client.resource(resource)
    fhir_client = fhir_client.page_size(page_size)
    fhir_client = fhir_client.include_only_properties(["id"])

    # loop by dates
    start_date = datetime.strptime("2022-01-01", "%Y-%m-%d")
    end_date = datetime.strptime("2022-01-10", "%Y-%m-%d")
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

    # for id_ in list_of_ids:
    #     print(id_)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    asyncio.run(print_hi('PyCharm'))

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
