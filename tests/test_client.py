import python_pachyderm

"""Tests generic client functionality"""

def test_client_init_with_default_host_port():
    # GIVEN a Pachyderm deployment
    # WHEN a client is created without specifying a host or port
    client = python_pachyderm.Client()
    # THEN the GRPC channel should reflect the default of localhost and port 30650
    assert client.address == 'localhost:30650'


def test_client_init_with_env_vars(monkeypatch):
    # GIVEN a Pachyderm deployment
    # WHEN environment variables are set for Pachyderm host and port
    monkeypatch.setenv('PACHD_ADDRESS', 'pachd.example.com:12345')
    #   AND a client is created without specifying a host or port
    client = python_pachyderm.Client()
    # THEN the GRPC channel should reflect the host and port specified in the environment variables
    assert client.address == 'pachd.example.com:12345'


def test_client_init_with_args():
    # GIVEN a Pachyderm deployment
    # WHEN a client is created with host and port arguments
    client = python_pachyderm.Client(host='pachd.example.com', port=54321)
    # THEN the GRPC channel should reflect the host and port specified in the arguments
    assert client.address == 'pachd.example.com:54321'