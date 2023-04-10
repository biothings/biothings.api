import math


class Schedule:
    def __init__(self, total, batch_size):
        self._batch_size = batch_size
        self._state = ""

        self.total = total
        self.scheduled = 0
        self.finished = 0

    @property
    def _batch(self):
        return math.ceil(self.scheduled / self._batch_size)

    @property
    def _batches(self):
        return math.ceil(self.total / self._batch_size)

    @property
    def _percentage(self):
        _percentage = self.scheduled / self.total * 100
        return "%.1f%%" % _percentage

    def suffix(self, string):
        return " ".join((string, "#%d/%d %s" % (self._batch, self._batches, self._percentage)))

    def completed(self):
        if self.finished != self.total:
            raise ValueError(self.finished, self.total)

    def __iter__(self):
        return self

    def __next__(self):
        if self.scheduled >= self.total:
            self._state = "pending, waiting for completion,"
            raise StopIteration()
        self.scheduled += self._batch_size
        if self.scheduled > self.total:
            self.scheduled = self.total
        self._state = self.suffix("running, on batch") + ","
        return self._batch

    def __str__(self):
        return " ".join(
            f"""
            <Schedule {"done" if self.finished >= self.total else self._state}
                total={self.total} scheduled={self.scheduled} finished={self.finished}>
            """.split()
        )


def test_01():
    schedule = Schedule(100, 10)
    for batch in schedule:
        print(batch)
        print(schedule)


def test_02():
    schedule = Schedule(25, 10)
    for batch in schedule:
        print(batch)
        print(schedule)
        print(schedule.suffix("Task"))


def test_03():
    schedule = Schedule(0, 10)
    for batch in schedule:
        print(batch)
        print(schedule)
        print(schedule.suffix("Task"))


def test_04():
    schedule = Schedule(1, 10)
    for batch in schedule:
        print(batch)
        print(schedule)
        print(schedule.suffix("Task"))


if __name__ == "__main__":
    test_02()
