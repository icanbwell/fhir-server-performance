import asyncio
import os
import time
from datetime import datetime, timedelta

from async_fhir_client import AsyncFhirClient


# def split_array(array, number_of_chunks) -> Generator[List[str], None, None]:
#     """
#     Splits an array into chunks
#     :param array:
#     :param number_of_chunks:
#     """
#     k, m = divmod(len(array), number_of_chunks)
#     return (array[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(number_of_chunks))


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
        self.start_date = datetime.strptime("2022-01-27", "%Y-%m-%d")
        self.end_date = datetime.strptime("2022-01-27", "%Y-%m-%d")

    async def print_hi(self, name):
        start_job = time.time()

        # Use a breakpoint in the code line below to debug your script.
        print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
        # from helix_fhir_client_sdk.fhir_client import FhirClient
        fhir_client = await self.create_fhir_client()
        resources = await fhir_client.get_resources_by_query_and_last_updated(
            concurrent_requests=50,
            page_size_for_retrieving_resources=100,
            page_size_for_retrieving_ids=self.page_size,
            start_date=self.start_date,
            end_date=self.end_date
        )

        end_job = time.time()
        print(f"====== Received {len(resources)} resources in {timedelta(seconds=end_job - start_job)} =======")

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
