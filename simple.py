import asyncio
from aiohttp import ClientSession, ClientResponse, ClientPayloadError
from dotenv import load_dotenv


async def load_data():
    use_data_streaming = True
    headers = {
        "Accept": "application/fhir+ndjson" if use_data_streaming else "application/json",
        "Content-Type": "application/fhir+json",
        "Accept-Encoding": "gzip,deflate",
    }

    async with ClientSession() as session:
        async with session.get('http://httpbin.org/get') as resp:
            print(resp.status)
            print(await resp.text())


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    load_dotenv()

    asyncio.run(load_data())
