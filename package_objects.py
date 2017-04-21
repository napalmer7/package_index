"""
Package Contents objects module.

This portion of the module handles the object translations and conversions.
"""

import re
import logging
from threading import Lock

logging.basicConfig(level=logging.INFO,
                    format="%(created)-15s %(msecs)d %(levelname)8s %(thread)d %(name)s %(message)s")
logger = logging.getLogger(__name__)

# Precompile the regex so it can be used repeated
request_regex = re.compile("(?P<cmd>[^\|]+)\|(?P<name>[^\|]+)\|(?P<depends>.*)")

# Build the command strings once and store as global refs
CMD_INDEX = "INDEX"
CMD_REMOVE = "REMOVE"
CMD_QUERY = "QUERY"

# Helper list to check if a command received is valid
VALID_COMMANDS = [CMD_INDEX, CMD_REMOVE, CMD_QUERY]


class DataPackageStore():
    # TODO The package store could sync out to a file for long term retention
    #      On initialization it can rebuild the maps from the file contents.
    def __init__(self, lock=Lock(), used_by={}, dependencies={}):
        """
        Construct a new DataPackageStore instance.

        :param lock: Multithreading lock object for synchronization.
        :type lock: threading.lock
        :param used_by: Map of package names to index packages that use it.
        :type used_by: dict
        :param dependencies: Map of package names to package dependencies.
        :type dependencies: dict
        """
        self.__data_lock = lock
        self.used_by = used_by
        self.dependencies = dependencies

    def find_package(self, key, locked=False):
        """
        Find an indexed package by name (key).

        :param key: Package name.
        :type key: string
        :param locked: Indicate if the store's data lock is already active.
        :type locked: bool

        :returns: List of dependencies for the package (may be empty) or None if not found.
        """
        try:
            if not locked:
                self.__data_lock.acquire()
            return self.dependencies.get(key, None)
        finally:
            if not locked:
                self.__data_lock.release()

    def add_package(self, package):
        """
        Attempt to add a new package to the indexed map.

        :param package: Package instance to be added to the index map.
        :type key: DataPackage

        :returns: Package name if successful, None if dependencies not met.
        """
        with self.__data_lock:
            if any(x not in self.dependencies for x in package.dependencies):
                logger.debug("Unable to add {}. Not all dependencies are indexed.".format(package.name))
                return None

            prev = self.dependencies.get(package.name, None)
            if prev:
                # Pre-existing packages may require dependency cleanup
                for item in [x for x in prev if x not in package.dependencies]:
                    # remove the parent map entry
                    self.used_by[item].remove(package.name)

            logger.debug("Synchronizing package {} dependencies and usage.".format(package.name))
            for x in package.dependencies:
                # add an entry for reverse lookup
                if package.name not in self.used_by:
                    self.used_by[x].append(package.name)

            # add the package
            self.dependencies[package.name] = package.dependencies
            if package.name not in self.used_by:
                self.used_by[package.name] = []
            return package.name

    def remove_package(self, key):
        """
        Remove an indexed package by name (key).

        :param key: Package name.
        :type key: string

        :returns: Package name (key) if removed or None if unable to remove.

        NOTE: If the package is not found this will be treated as a successful removal.
        """
        with self.__data_lock:
            val = self.find_package(key, True)
            if val is None:
                logger.debug("Package {} not found in index.".format(key))
                return key
            else:
                # Check to see if any index packages still need this package
                used = self.used_by.get(key, None)
                if not used:
                    logger.debug("Removing {} and synchronizing usage maps.".format(key))
                    # Clear up entries in the reverse lookup
                    for x in val:
                        # Update the dependency's parent set
                        self.used_by[x].remove(key)

                    # Pop the values off the two maps
                    self.dependencies.pop(key, None)
                    self.used_by.pop(key, None)
                    return key
                else:
                    logger.debug("Unable to remove {}: {} packages used it.".format(key, len(used)))
                    return None

    def print_state(self):
        """Print the current state of the DataPackageStore.

        This method prints the current dependencies and usage maps
        and can be helpful when debugging the indexed data.
        """
        with self.__data_lock:
            logger.debug("Package Store Dependencies")
            for k, v in self.dependencies.items():
                logger.debug("    {}: [{}]".format(k, v))

            logger.debug("Package Store Used By Sets")
            for k, v in self.used_by.items():
                logger.debug("    {}: [{}]".format(k, v))


class DataPackage:
    """Representation of an individual Package object."""

    def __init__(self, req_string):
        """
        Construct a new DataPackage instance.

        :param req_string: Raw string package command
        :type req_string: string
        """

        if not req_string:
            # Null or empty string
            raise ValueError('Invalid request string detected. {}'.format(req_string))

        result = request_regex.match(req_string)

        if not result:
            # incorrectly formatted message received
            raise ValueError('Invalid request string detected. {}'.format(req_string))

        self.command = result.group('cmd')
        if self.command not in VALID_COMMANDS:
            raise ValueError('Invalid command ({}). Valid commands are {}'.format(self.command,
                             VALID_COMMANDS))

        self.name = result.group('name')
        deps = result.group('depends')
        if deps:
            self.dependencies = deps.split(',')
        else:
            self.dependencies = []

    def is_query(self):
        """Check if the command was a QUERY"""
        return self.command == CMD_QUERY

    def is_index(self):
        """Check if the command was an INDEX"""
        return self.command == CMD_INDEX

    def is_remove(self):
        """Check if the command was a REMOVE"""
        return self.command == CMD_REMOVE


if __name__ == '__main__':
    # Allow this module to execute a main to test the parsing logic for packages.
    valid_data = """INDEX|cloog|gmp,isl,pkg-config
INDEX|ceylon|
QUERY|cloog|
REMOVE|cloog|
QUERY|cloog|
QUERY|ceylon|
INDEX|clooper|ceylon
REMOVE|ceylon|
REMOVE|clooper|
REMOVE|ceylon|"""

    invalid_data = """
INDeX|ceylon|
QUERY,cloog|
remove|cloog|
QUERY|cloog|(c,b,a)
REMOVE,clooper,
REMOVE|clooper
"""

    for s in valid_data.split('\n'):
        assert DataPackage(s) is not None

    fail_count = 0
    items = invalid_data.split('\n')
    for s in items:
        p = None
        try:
            p = DataPackage(s)
        except:
            # expect them to fail
            fail_count += 1
            pass
    assert fail_count == len(items)-1
