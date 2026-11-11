import pytest

from de_playground.config import Settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings.from_env()
