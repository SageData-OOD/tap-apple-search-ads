import datetime
import jwt
import singer

logger = singer.get_logger()
DEFAULT_VALUES = {
    "audience": "https://appleid.apple.com",
    "algorithm": "ES256"
}


class ClientSecret:
    _max_expiration_time = datetime.timedelta(days=180).total_seconds()

    def __init__(self, issued_at_timestamp, expiration_time):
        # client secret specific
        self.issued_at_timestamp = issued_at_timestamp
        self.expiration_time = expiration_time

        # payload
        self.audience = None
        self.team_id = None
        self.client_id = None

        # headers
        self.algorithm = None
        self.key_id = None

    @property
    def expiration_timestamp(self) -> int:

        if self.expiration_time > self._max_expiration_time:
            raise ValueError(
                (
                    "expiration_time ([{}] seconds) may not exceed 180 days "
                    "from issue timestamp ([{}] seconds)"
                ).format(self.expiration_time, self._max_expiration_time)
            )

        return self.issued_at_timestamp + self.expiration_time

    def set_payload(self, client_id, team_id, audience=DEFAULT_VALUES["audience"]):
        self.client_id = client_id
        self.team_id = team_id
        self.audience = audience

    def set_headers(self, key_id, algorithm=DEFAULT_VALUES["algorithm"]):
        self.key_id = key_id
        self.algorithm = algorithm

    def value(self, private_key: str) -> str:
        jwt_payload = {
            "sub": self.client_id,
            "aud": self.audience,
            "iat": self.issued_at_timestamp,
            "exp": self.expiration_timestamp,
            "iss": self.team_id
        }

        jwt_headers = {
            "alg": self.algorithm,
            "kid": self.key_id
        }

        logger.debug(
            "payload: [%s], headers: [%s], algorithm: [%s]",
            jwt_payload,
            jwt_headers,
            self.algorithm
        )

        client_secret = jwt.encode(
            payload=jwt_payload,
            headers=jwt_headers,
            algorithm=self.algorithm,
            key=private_key,
        )

        return client_secret
