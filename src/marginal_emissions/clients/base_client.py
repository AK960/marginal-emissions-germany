from typing import Optional

### Factory ###
class EnergyDataFactory:
    pass

class EnergyDataClient:
    """
    Base Client for different Energy Data Platform APIs
    """

    def __init__(self, api_key: str, base_url: Optional[str]):
        self.api_key = api_key
        self.base_url = base_url

        if self.api_key is None:
            raise TypeError("API key cannot be None")
        if self.base_url is None:
            raise TypeError("Base URL cannot be None")
