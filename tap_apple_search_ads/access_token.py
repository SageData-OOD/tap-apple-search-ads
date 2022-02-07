import requests
import singer

logger = singer.get_logger()


class AccessToken:
    def __init__(self, client_id, url):
        self.client_id = client_id
        self.url = url

    def value(self, client_secret):
        logger.debug(
            "url: [%s], headers: [%s], params: [%s]",
            self.url,
            self.headers,
            self.params,
        )

        response = requests.post(
            self.url,
            headers=self.headers,
            params=self.params(client_secret),
        )

        data = response.json()

        return {
            "access_token": data["access_token"],
            "token_type": data["token_type"],
            "expires_in": data["expires_in"]
        }

    @property
    def headers(self):
        return {
            "Host": "appleid.apple.com",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def params(self, client_secret):
        return {
            "client_id": self.client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scope": "searchadsorg"
        }
