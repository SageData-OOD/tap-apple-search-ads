import singer

logger = singer.get_logger()


class RequestHeaders:
    def __init__(self, org_id):
        self.org_id = org_id

    def value(self, access_token):
        if access_token["token_type"] != "Bearer":
            message = "Unexpected token_type [{}], expected Bearer"
            logger.error(message, access_token["token_type"])
            raise RuntimeError(message.format(access_token["token_type"]))

        return {
            "Authorization": "{} {}".format(
                access_token["token_type"], access_token["access_token"]
            ),
            "X-AP-Context": "orgId={}".format(self.org_id),
        }
