"""Check schema consumers with both mainstream typecheckers."""

import copy
from pathlib import Path
import subprocess
import sys

import pytest

from zschema.keys import _NO_ARG
from zschema.leaves import String


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("checker", ["mypy", "pyright"])
def test_schema_typing(checker):
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            checker,
            "zschema/fixtures/typing_schema.py",
            "zschema/example.py",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_sentinel_preserves_defaults():
    class DefaultString(String):
        DOC = "Inherited documentation"
        REQUIRED = True

    assert not _NO_ARG
    assert copy.copy(_NO_ARG) is _NO_ARG
    assert copy.deepcopy(_NO_ARG) is _NO_ARG
    assert DefaultString().doc == "Inherited documentation"
    assert DefaultString().required is True
    assert DefaultString(doc=None).doc is None
    assert DefaultString(required=False).required is False
    assert copy.deepcopy(DefaultString()).doc == "Inherited documentation"
