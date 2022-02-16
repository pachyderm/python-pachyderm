import io
import os
import json
from pathlib import Path

import pytest
import python_pachyderm
from tests import util

"""Tests generic client functionality"""

CERT = """
-----BEGIN CERTIFICATE-----
MIIGfjCCBGagAwIBAgIJAJeg/PrX5Sj9MA0GCSqGSIb3DQEBCwUAMIGBMQswCQYD
VQQGEwJVUzETMBEGA1UECAwKQ2FsaWZvcm5pYTEWMBQGA1UEBwwNU2FuIEZyYW5j
aXNjbzEPMA0GA1UECgwGQmFkU1NMMTQwMgYDVQQDDCtCYWRTU0wgVW50cnVzdGVk
IFJvb3QgQ2VydGlmaWNhdGUgQXV0aG9yaXR5MB4XDTE2MDcwNzA2MzEzNVoXDTM2
MDcwMjA2MzEzNVowgYExCzAJBgNVBAYTAlVTMRMwEQYDVQQIDApDYWxpZm9ybmlh
MRYwFAYDVQQHDA1TYW4gRnJhbmNpc2NvMQ8wDQYDVQQKDAZCYWRTU0wxNDAyBgNV
BAMMK0JhZFNTTCBVbnRydXN0ZWQgUm9vdCBDZXJ0aWZpY2F0ZSBBdXRob3JpdHkw
ggIiMA0GCSqGSIb3DQEBAQUAA4ICDwAwggIKAoICAQDKQtPMhEH073gis/HISWAi
bOEpCtOsatA3JmeVbaWal8O/5ZO5GAn9dFVsGn0CXAHR6eUKYDAFJLa/3AhjBvWa
tnQLoXaYlCvBjodjLEaFi8ckcJHrAYG9qZqioRQ16Yr8wUTkbgZf+er/Z55zi1yn
CnhWth7kekvrwVDGP1rApeLqbhYCSLeZf5W/zsjLlvJni9OrU7U3a9msvz8mcCOX
fJX9e3VbkD/uonIbK2SvmAGMaOj/1k0dASkZtMws0Bk7m1pTQL+qXDM/h3BQZJa5
DwTcATaa/Qnk6YHbj/MaS5nzCSmR0Xmvs/3CulQYiZJ3kypns1KdqlGuwkfiCCgD
yWJy7NE9qdj6xxLdqzne2DCyuPrjFPS0mmYimpykgbPnirEPBF1LW3GJc9yfhVXE
Cc8OY8lWzxazDNNbeSRDpAGbBeGSQXGjAbliFJxwLyGzZ+cG+G8lc+zSvWjQu4Xp
GJ+dOREhQhl+9U8oyPX34gfKo63muSgo539hGylqgQyzj+SX8OgK1FXXb2LS1gxt
VIR5Qc4MmiEG2LKwPwfU8Yi+t5TYjGh8gaFv6NnksoX4hU42gP5KvjYggDpR+NSN
CGQSWHfZASAYDpxjrOo+rk4xnO+sbuuMk7gORsrl+jgRT8F2VqoR9Z3CEdQxcCjR
5FsfTymZCk3GfIbWKkaeLQIDAQABo4H2MIHzMB0GA1UdDgQWBBRvx4NzSbWnY/91
3m1u/u37l6MsADCBtgYDVR0jBIGuMIGrgBRvx4NzSbWnY/913m1u/u37l6MsAKGB
h6SBhDCBgTELMAkGA1UEBhMCVVMxEzARBgNVBAgMCkNhbGlmb3JuaWExFjAUBgNV
BAcMDVNhbiBGcmFuY2lzY28xDzANBgNVBAoMBkJhZFNTTDE0MDIGA1UEAwwrQmFk
U1NMIFVudHJ1c3RlZCBSb290IENlcnRpZmljYXRlIEF1dGhvcml0eYIJAJeg/PrX
5Sj9MAwGA1UdEwQFMAMBAf8wCwYDVR0PBAQDAgEGMA0GCSqGSIb3DQEBCwUAA4IC
AQBQU9U8+jTRT6H9AIFm6y50tXTg/ySxRNmeP1Ey9Zf4jUE6yr3Q8xBv9gTFLiY1
qW2qfkDSmXVdBkl/OU3+xb5QOG5hW7wVolWQyKREV5EvUZXZxoH7LVEMdkCsRJDK
wYEKnEErFls5WPXY3bOglBOQqAIiuLQ0f77a2HXULDdQTn5SueW/vrA4RJEKuWxU
iD9XPnVZ9tPtky2Du7wcL9qhgTddpS/NgAuLO4PXh2TQ0EMCll5reZ5AEr0NSLDF
c/koDv/EZqB7VYhcPzr1bhQgbv1dl9NZU0dWKIMkRE/T7vZ97I3aPZqIapC2ulrf
KrlqjXidwrGFg8xbiGYQHPx3tHPZxoM5WG2voI6G3s1/iD+B4V6lUEvivd3f6tq7
d1V/3q1sL5DNv7TvaKGsq8g5un0TAkqaewJQ5fXLigF/yYu5a24/GUD783MdAPFv
gWz8F81evOyRfpf9CAqIswMF+T6Dwv3aw5L9hSniMrblkg+ai0K22JfoBcGOzMtB
Ke/Ps2Za56dTRoY/a4r62hrcGxufXd0mTdPaJLw3sJeHYjLxVAYWQq4QKJQWDgTS
dAEWyN2WXaBFPx5c8KIW95Eu8ShWE00VVC3oA4emoZ2nrzBXLrUScifY6VaYYkkR
2O2tSqU8Ri3XRdgpNPDWp8ZL49KhYGYo3R/k98gnMHiY5g==
-----END CERTIFICATE-----
"""


def test_check_config_order(mocker):
    env_config = json.loads(
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
    spout_config = json.loads(
        """
      {
        "v2": {
          "active_context": "local",
          "contexts": {
            "local": {
              "pachd_address": "[::1]:80",
              "server_cas": "foo",
              "session_token": "bar",
              "active_transaction": "baz"
            }
          }
        }
      }
    """
    )
    local_config = json.loads(
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
    for config in (env_config, spout_config, local_config):
        config["v2"]["contexts"]["local"]["server_cas"] = CERT

    mocker.patch(
        "python_pachyderm.experimental.client._Client._check_pach_config_env_var",
        return_value=env_config,
    )
    mocker.patch(
        "python_pachyderm.experimental.client._Client._check_pach_config_spout",
        return_value=spout_config,
    )
    mocker.patch(
        "python_pachyderm.experimental.client._Client._check_pach_config_local",
        return_value=local_config,
    )

    # Retrieves from env var config file
    client = python_pachyderm.experimental.Client()
    assert client.address == "172.17.0.6:30650"

    # Retrieves from spout config file
    mocker.patch(
        "python_pachyderm.experimental.client._Client._check_pach_config_env_var",
        return_value=None,
    )
    client = python_pachyderm.experimental.Client()
    assert client.address == "::1:80"

    # Retrieves from local config file
    mocker.patch(
        "python_pachyderm.experimental.client._Client._check_pach_config_spout",
        return_value=None,
    )
    client = python_pachyderm.experimental.Client()
    assert client.address == "localhost:10101"

    # Executes default constructor behavior
    mocker.patch(
        "python_pachyderm.experimental.client._Client._check_pach_config_local",
        return_value=None,
    )
    client = python_pachyderm.experimental.Client()
    assert client.address == "localhost:30650"


def test_parse_config():
    (
        host,
        port,
        pachd_address,
        auth_token,
        root_certs,
        transaction_id,
        tls,
    ) = python_pachyderm.Client._parse_config(
        json.loads(
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
    assert host == "172.17.0.6"
    assert port == 30650
    assert pachd_address == "grpcs://172.17.0.6:30650"
    assert auth_token == "bar"
    assert root_certs == b"foo"
    assert transaction_id == "baz"
    assert tls is True

    (
        host,
        port,
        pachd_address,
        auth_token,
        root_certs,
        transaction_id,
        tls,
    ) = python_pachyderm.Client._parse_config(
        json.loads(
            """
        {
          "v2": {
            "active_context": "local",
            "contexts": {
              "local": {
                "port_forwarders": {
                  "pachd": 10101
                },
                "server_cas": "foo",
                "session_token": "bar"
              }
            }
          }
        }
    """
        )
    )
    assert host == "localhost"
    assert port == 10101
    assert pachd_address == "grpc://localhost:10101"
    assert auth_token == "bar"
    assert root_certs is None
    assert transaction_id is None
    assert tls is False


def test_client_init_with_args():
    client = python_pachyderm.Client(host="pachd.example.com", port=54321)
    assert client.address == "pachd.example.com:54321"


def test_client_new_in_cluster_missing_envs():
    with pytest.raises(Exception):
        python_pachyderm.Client.new_in_cluster()


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
    assert client.root_certs == b"foo"
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

    # empty context should default to localhost:30650
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
