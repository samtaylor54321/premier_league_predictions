import pytest


@pytest.mark.xfail
def test_failing():
    assert (1, 2, 3) == [3, 2, 1]


def test_passing():
    assert (1, 2, 3) == (1, 2, 3)
