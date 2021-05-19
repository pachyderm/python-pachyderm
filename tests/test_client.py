import io

import pytest
import python_pachyderm
from tests import util

"""Tests generic client functionality"""


def test_client_init_with_default_host_port():
    client = python_pachyderm.Client()
    assert client.address == "localhost:30650"


def test_client_init_with_args():
    client = python_pachyderm.Client(host="pachd.example.com", port=54321)
    assert client.address == "pachd.example.com:54321"


def test_client_new_in_cluster_missing_envs():
    with pytest.raises(Exception):
        client = python_pachyderm.Client.new_in_cluster()


def test_client_new_from_pachd_address():
    with pytest.raises(ValueError):
        python_pachyderm.Client.new_from_pachd_address("grpc://user@pachyderm.com:80")

    with pytest.raises(ValueError):
        python_pachyderm.Client.new_from_pachd_address(
            "grpc://user:pass@pachyderm.com:80"
        )

    with pytest.raises(ValueError):
        python_pachyderm.Client.new_from_pachd_address("grpc://pachyderm.com:80/")

    with pytest.raises(ValueError):
        python_pachyderm.Client.new_from_pachd_address("grpc://pachyderm.com:80/?foo")

    with pytest.raises(ValueError):
        python_pachyderm.Client.new_from_pachd_address("grpc://pachyderm.com:80/#foo")

    client = python_pachyderm.Client.new_from_pachd_address("http://pachyderm.com:80")
    assert client.address == "pachyderm.com:80"

    client = python_pachyderm.Client.new_from_pachd_address(
        "https://pachyderm.com:80", root_certs=b"foo"
    )
    assert client.address == "pachyderm.com:80"
    assert client.root_certs is not None

    client = python_pachyderm.Client.new_from_pachd_address(
        "https://pachyderm.com:80", root_certs=b"foo"
    )
    assert client.address == "pachyderm.com:80"
    assert client.root_certs is not None

    client = python_pachyderm.Client.new_from_pachd_address("grpcs://pachyderm.com:80")
    assert client.address == "pachyderm.com:80"
    assert client.root_certs is not None

    client = python_pachyderm.Client.new_from_pachd_address(
        "grpcs://[::1]:80", root_certs=b"foo"
    )
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


def test_client_new_from_config():
    # should fail because there's no active context
    with pytest.raises(python_pachyderm.ConfigError):
        python_pachyderm.Client.new_from_config(
            config_file=io.StringIO(
                """
            {
              "v2": {
                "contexts": {
                  "local": { }
                }
              }
            }
        """
            )
        )

    # should fail since the context 'local' is missing
    with pytest.raises(python_pachyderm.ConfigError):
        python_pachyderm.Client.new_from_config(
            config_file=io.StringIO(
                """
            {
              "v2": {
                "active_context": "local",
                "contexts": { }
              }
            }
        """
            )
        )

    # check that pachd address and other context fields are respected
    client = python_pachyderm.Client.new_from_config(
        config_file=io.StringIO(
            """
        {
          "v2": {
            "active_context": "local",
            "contexts": {
              "local": {
                "pachd_address": "grpcs://172.17.0.6:30650",
                "server_cas": "foo",
                "session_token": "bar",
                "active_transaction": "baz"
              }
            }
          }
        }
    """
        )
    )
    assert client.address == "172.17.0.6:30650"
    assert client.root_certs == "foo"
    assert client.auth_token == "bar"
    assert client.transaction_id == "baz"

    # port forwarders should be respected
    client = python_pachyderm.Client.new_from_config(
        config_file=io.StringIO(
            """
        {
          "v2": {
            "active_context": "local",
            "contexts": {
              "local": {
                "port_forwarders": {
                  "pachd": 10101
                }
              }
            }
          }
        }
    """
        )
    )
    assert client.address == "localhost:10101"

    # empty context should default ot localhost:30650
    client = python_pachyderm.Client.new_from_config(
        config_file=io.StringIO(
            """
        {
          "v2": {
            "active_context": "local",
            "contexts": {
              "local": { }
            }
          }
        }
    """
        )
    )
    assert client.address == "localhost:30650"

    # verifies that a bad cluster ID triggers an error
    with pytest.raises(python_pachyderm.BadClusterDeploymentID):
        client = python_pachyderm.Client.new_from_config(
            config_file=io.StringIO(
                """
            {
              "v2": {
                "active_context": "local",
                "contexts": {
                  "local": {
                    "cluster_deployment_id": "foobar"
                  }
                }
              }
            }
        """
            )
        )

    # verifies that a good cluster ID does not trigger an error
    client = python_pachyderm.Client.new_from_config(
        config_file=io.StringIO(
            """
        {
          "v2": {
            "active_context": "local",
            "contexts": {
              "local": {
                "cluster_deployment_id": "%s"
              }
            }
          }
        }
    """
            % util.get_cluster_deployment_id()
        )
    )
    assert client.address == "localhost:30650"
