import mock
import pytest
from botocore.exceptions import ClientError

from ..stream import StreamWriter

def test_StreamWriter__init__exception_handling():
    with mock.patch('boto3.client') as mock_client:
        error_response = {'Error': {'Code': 'ResourceInUseException'}}
        mock_client.create_stream = mock.Mock(side_effect=lambda: ClientError(error_response))

        StreamWriter('blah')
        assert mock_client.called, 'Sanity'



