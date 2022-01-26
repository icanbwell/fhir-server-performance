# This is a sample Python script.

# Press ⇧F10 to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import os
from datetime import datetime, timedelta
from typing import Optional, List

from helix_fhir_client_sdk.filters.base_filter import BaseFilter


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


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
    from helix_fhir_client_sdk.fhir_client import FhirClient
    server_url = "https://fhir.icanbwell.com/4_0_0"
    auth_client_id = os.environ.get("FHIR_CLIENT_ID")
    assert auth_client_id
    auth_client_secret = os.environ.get("FHIR_CLIENT_SECRET")
    assert auth_client_secret
    resource = "AuditEvent"
    client = "medstar"
    auth_scopes = [f"user/{resource}.read", f"access/{client}.*"]
    fhir_client: FhirClient = FhirClient()
    fhir_client = fhir_client.url(server_url)
    fhir_client = fhir_client.client_credentials(auth_client_id, auth_client_secret)
    fhir_client = fhir_client.auth_scopes(auth_scopes)
    fhir_client = fhir_client.resource(resource)
    greater_than = datetime.strptime("2022-01-08", "%Y-%m-%d")
    less_than = greater_than + timedelta(days=1)
    fhir_client = fhir_client.filter(
        [
            LastUpdatedFilter(
                less_than=less_than,
                greater_than=greater_than
            )
        ]
    )
    fhir_client = fhir_client.page_size(10).page_number(2)
    fhir_client = fhir_client.include_only_properties(["id"])

    result = fhir_client.get()

    import json
    resource_list = json.loads(result.responses)
    for resource in resource_list:
        print(resource['id'])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
