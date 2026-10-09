import pytest

from src.transform.session import get_spark


@pytest.fixture(scope="session")
def spark():
    """One Spark session for the whole test run (starting Spark takes seconds)."""
    session = get_spark("tests")
    yield session
    session.stop()