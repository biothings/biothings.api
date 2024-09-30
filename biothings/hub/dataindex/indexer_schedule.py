import math
from typing import Union


class SchedulerMismatchError(Exception):
    """
    Exception for indicating a mismatch in the number of expected
    documents versus the total number of actually uploaded documents
    to the target database
    """

    def __init__(self, completed_documents: int, expected_documents: int):

        self.completed_documents = completed_documents
        self.expected_documents = expected_documents

        message = (
            f"Difference found between the number of completed documents [{self.completed_documents}] "
            f"in the indexing process and the number of expected documents [{self.expected_documents}] "
            "to be indexed based off the collection size. "
            "This error can occur if the documents uploaded don't have an _id field with MongoDB. "
            "Please verify the document structure at the upload phase"
        )
        super().__init__(message)


class Schedule:
    def __init__(self, total: int, batch_size: int):
        self._batch_size = batch_size
        self.total = total

        self._state = "initialization"
        self.scheduled = 0
        self.finished = 0

    @classmethod
    def _calculate_percentage(
        self, numerator: Union[int, float], denominator: Union[int, float], ceiling: bool
    ) -> float:
        """
        Calculate the percentage given two numbers and return that value

        -> numerator / denominator

        if ceiling is applied then we apply the ceiling function to the result
        -> ceiling[numerator / denominator]
        """
        try:
            percentage = numerator / denominator
        except ZeroDivisionError as div_error:
            raise div_error

        if ceiling:
            percentage = math.ceil(percentage)
        return percentage

    @property
    def _batch(self):
        return self._calculate_percentage(self.scheduled, self._batch_size, True)

    @property
    def _batches(self):
        return self._calculate_percentage(self.total, self._batch_size, True)

    @property
    def _percentage(self):
        _percentage = self._calculate_percentage(self.scheduled, self.total, False) * 100
        return "%.1f%%" % _percentage

    def suffix(self, suffix_value: str) -> str:
        batch_repr = f"#{self._batch}/{self._batches} {self._percentage}"
        suffix_repr = f"{suffix_value} {batch_repr}"
        return suffix_repr

    def completed(self):
        if not self.finished == self.total:
            raise SchedulerMismatchError(self.finished, self.total)
        self._state = "done"

    def __iter__(self):
        return self

    def __next__(self):
        if self.scheduled >= self.total:
            self._state = "pending, waiting for completion,"
            raise StopIteration()

        self.scheduled += self._batch_size
        self.scheduled = min(self.scheduled, self.total)
        self._state = self.suffix("running, on batch") + ","
        return self._batch

    def __str__(self) -> str:
        schedule_str = (
            f"<schedule {self._state} "
            f"total={self.total} "
            f"scheduled={self.scheduled} "
            f"finished={self.finished}>"
        )
        return schedule_str
