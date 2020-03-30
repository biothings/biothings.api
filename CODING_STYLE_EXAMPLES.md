# Common coding style issues

1. Unnecessary list comprehension - 'tuple' can take a generator

        tuple([i for i in range(10)])    # NO

        tuple(i for i in range(10))      # YES

2. Logging statement uses '%'

        logging.info("Add '%s' to watcher" % event.pathname)   # NO

        logging.info("Add '%s' to watcher", event.pathname)    # YES

3. Always use `cls` for the first argument to class methods

        @classmethod
        def save_cmd(klass, _id, cmd):       # NO
            pass

        @classmethod
        def save_cmd(cls, _id, cmd):         # YES
            pass

4. Using dict comprehension and set comprehension

        a_dict = dict([(x, 1) for x in range(10)])    # NO

        a_dict = {x: 1 for x in range(10)}            # YES

        a_dict = set([d[i] for i in range(10)])       # NO

        a_dict = {x[i] for i in range(10)}            # YES

5. Always add a docstring to a class

   * If a class is a subclass of another class, and there is no docstring provided, Sphinx will
     pick up the docstring from the parent class, which may not be what you want to display in
     the auto-generated docs.

   * If a class is not a subclass, or a function/method, and has no docstring, it will be skipped
     by Sphinx, so it won't be listed in the auto-generated docs.
