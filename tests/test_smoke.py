"""Smoke test: the package imports. Keeps CI green from day one.

Replace/extend once real functionality lands (see docs/HANDOFF.md).
"""


def test_package_imports():
    import edutap.webhook_heidi

    assert edutap.webhook_heidi.__doc__
