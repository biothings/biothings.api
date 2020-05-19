
def run_once():
    """
    should_run_task_1 = run_once()
    print(should_run_task_1()) -> True
    print(should_run_task_1()) -> False
    print(should_run_task_1()) -> False
    print(should_run_task_1()) -> False

    should_run_task_2 = run_once()
    print(should_run_task_2('2a')) -> True
    print(should_run_task_2('2b')) -> True
    print(should_run_task_2('2a')) -> False
    print(should_run_task_2('2b')) -> False
    ...
    """

    has_run = set()

    def should_run(identifier=None):

        if identifier in has_run:
            return False

        has_run.add(identifier)
        return True

    return should_run
