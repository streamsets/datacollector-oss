import pytest
from click.testing import CliRunner
from sdc import cli


@pytest.fixture
def runner():
    return CliRunner()


def test_cli(runner):
    result = runner.invoke(cli.cli)
    assert result.exit_code == 0
    assert not result.exception
    assert 'Usage' in result.output.strip()
