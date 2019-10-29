import string
import random

def random_string(n):
    return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))

def test_repo_name(test_name, prefix=None, suffix=None):
    prefix = "" if prefix is None else "{}-".format(prefix)
    suffix = suffix or random_string(6)
    return "{}{}-{}".format(prefix, test_name, suffix)

def create_test_repo(client, test_name, prefix=None, suffix=None):
    repo_name = test_repo_name(test_name, prefix=prefix, suffix=suffix)
    client.create_repo(repo_name, "python_pachyderm test repo for {}".format(test_name))
    return repo_name
