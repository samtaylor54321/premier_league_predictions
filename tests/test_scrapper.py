import pytest


def test_failing():
    assert (1, 2, 3) == [3, 2, 1]


def test_passing():
    with pytest.raises(ValueError):
        assert (1, 2, 3) == (1, 2, 3)
