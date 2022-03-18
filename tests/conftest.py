""" Ref: https://docs.pytest.org/en/latest/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files

    The conftest.py file serves as a means of providing fixtures for an entire directory.
    Fixtures defined in a conftest.py can be used by any test in that package without needing to import them
    (pytest will automatically discover them).
"""

# make sure hub is config before testing.
import biothings.hub
