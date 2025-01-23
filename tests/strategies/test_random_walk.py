import spiderexpress.strategies.random_walk

from pathlib import Path
from spiderexpress.spider import Spider


def test_spider_with_random_walk():
    """Should instantiate a spider."""
    spider = Spider()

    assert spider is not None
    assert spider.is_idle()

    spider.start(Path("tests/stubs/sevens_grader_random_walk_test.pe.yml"))

    assert spider.is_stopped()
    assert spider.configuration is not None
    assert spider.iteration > 0