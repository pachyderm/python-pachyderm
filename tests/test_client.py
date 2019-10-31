import pytest
import python_pachyderm

"""Tests generic client functionality"""

def test_client_init_with_default_host_port():
    client = python_pachyderm.Client()
    assert client.address == 'localhost:30650'


def test_client_init_with_args():
    client = python_pachyderm.Client(host='pachd.example.com', port=54321)
    assert client.address == 'pachd.example.com:54321'


def test_client_new_in_cluster(monkeypatch):
    monkeypatch.setenv('PACHD_SERVICE_HOST', 'pachd.example.com')
    monkeypatch.setenv('PACHD_SERVICE_PORT', '12345')
    client = python_pachyderm.Client.new_in_cluster()
    assert client.address == 'pachd.example.com:12345'


def test_client_new_in_cluster_missing_envs():
    with pytest.raises(Exception):
        client = python_pachyderm.Client.new_in_cluster()


def test_client_new_from_pachd_address(monkeypatch):
    monkeypatch.setenv('PACHD_SERVICE_HOST', 'pachd.example.com')
    monkeypatch.setenv('PACHD_SERVICE_PORT', '12345')
    client = python_pachyderm.Client.new_in_cluster()
    assert client.address == 'pachd.example.com:12345'


def test_client_new_from_pachd_address():
    with pytest.raises(ValueError):
        python_pachyderm.Client.new_from_pachd_address("grpc://user@pachyderm.com:80")

    with pytest.raises(ValueError):
        python_pachyderm.Client.new_from_pachd_address("grpc://user:pass@pachyderm.com:80")

    with pytest.raises(ValueError):
        python_pachyderm.Client.new_from_pachd_address("grpc://pachyderm.com:80/")

    with pytest.raises(ValueError):
        python_pachyderm.Client.new_from_pachd_address("grpc://pachyderm.com:80/?foo")

    with pytest.raises(ValueError):
        python_pachyderm.Client.new_from_pachd_address("grpc://pachyderm.com:80/#foo")

    client = python_pachyderm.Client.new_from_pachd_address("http://pachyderm.com:80")
    assert client.address == "pachyderm.com:80"

    client = python_pachyderm.Client.new_from_pachd_address("https://pachyderm.com:80", root_certs=b"foo")
    assert client.address == "pachyderm.com:80"
    assert client.root_certs is not None

    client = python_pachyderm.Client.new_from_pachd_address("https://pachyderm.com:80", root_certs=b"foo")
    assert client.address == "pachyderm.com:80"
    assert client.root_certs is not None

    client = python_pachyderm.Client.new_from_pachd_address("grpcs://pachyderm.com:80")
    assert client.address == "pachyderm.com:80"
    assert client.root_certs is not None

    client = python_pachyderm.Client.new_from_pachd_address("grpcs://[::1]:80", root_certs=b"foo")
    assert client.address == "::1:80"
    assert client.root_certs is not None

    client = python_pachyderm.Client.new_from_pachd_address("grpc://pachyderm.com")
    assert client.address == "pachyderm.com:30650"

    client = python_pachyderm.Client.new_from_pachd_address("127.0.0.1")
    assert client.address == "127.0.0.1:30650"

    client = python_pachyderm.Client.new_from_pachd_address("127.0.0.1:80")
    assert client.address == "127.0.0.1:80"

    client = python_pachyderm.Client.new_from_pachd_address("[::1]")
    assert client.address == "::1:30650"

    client = python_pachyderm.Client.new_from_pachd_address("[::1]:80")
    assert client.address == "::1:80"
