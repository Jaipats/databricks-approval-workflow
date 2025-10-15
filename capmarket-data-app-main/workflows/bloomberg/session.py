import json
import logging
import time

import jwt
from requests.exceptions import HTTPError
from requests_oauthlib import OAuth2Session

LOG = logging.getLogger(__name__)


class DLRestApiSession(OAuth2Session):
    """Custom session class for making requests to a DL REST API using OAuth2 authentication."""

    oauth2_endpoint = "https://bsso.blpprofessional.com/ext/api/as/token.oauth2"
    instances = dict()

    def __new__(cls, credentials, **kwargs):
        """
        Provide a global point of access to the instance of this class.
        Ensure only one instance per client_id is created.
        """
        client_id = credentials["client_id"]
        if not cls.instances.get(client_id):
            cls.instances[client_id] = super().__new__(cls)

        return cls.instances[client_id]

    def __init__(self, credentials, *args, **kwargs):
        """
        Initialize a DLRestApiSession instance.
        """
        super().__init__(*args, **kwargs)
        self.credentials = credentials
        self._token_expires_at = 0
        self._request_token()

    def _request_token(self):
        """
        Fetch an OAuth2 access token by making a request to the token endpoint.
        """
        self.access_token = self.fetch_token(
            token_url=self.oauth2_endpoint, client_secret=self.credentials["client_secret"]
        ).get("access_token")

        decoded_data = jwt.decode(
            jwt=self.access_token, options={"verify_signature": False}
        )
        self._token_expires_at = decoded_data["exp"]

    def _ensure_valid_token(self):
        """
        Ensure that OAuth2 access token is valid. Fetch a new token if the token is expired.
        """
        if not self.access_token:
            raise RuntimeError("\n\tAccess token error")

        valid_min_duration_threshold = 5
        seconds_until_expiration = self._token_expires_at - time.time()

        if valid_min_duration_threshold >= seconds_until_expiration:
            self._request_token()

        return

    def request(self, *args, **kwargs):
        """
        Override the parent class method to request OAuth2 token.
        :return: response object from the API request
        """
        if kwargs.get("url") != self.oauth2_endpoint:
            self._ensure_valid_token()

        return super().request(*args, **kwargs)

    def send(self, request, **kwargs):
        """
        Override the parent class method to log request and response information.
        :param request: prepared request object
        :return: response object from the API request
        """
        # This is a required header for each call to DL Rest API.
        request.headers['api-version'] = '2'
        response = super().send(request, **kwargs)
        try:
            response.raise_for_status()
        except HTTPError as exc:
            LOG.error('\n\tUnexpected response status code: {c}\nDetails: {r}'.format(
                c=str(response.status_code), r=response.json()))
            raise exc
        else:
            # Filter out SSE and file download responses.
            if response.headers.get(
                    "Content-Type"
            ) != "text/event-stream" and not response.headers.get(
                "Content-Disposition"
            ) and response.content:
                LOG.info("Response content: %s", json.dumps(response.json(), indent=2))

        return response
