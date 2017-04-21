"""
Package Index Service module.

This module creates a TCP service to listen for index, query and removal requests.

The general concept is to have a long running service that will create asynchronous
workers that will manage a single client connection. The front end service receives
a client connection request and farms that work to a secondary data port that will
be processed by a independent worker/thread. Depending on client model and connection
expectations the workers and ports can be recycled for another client's use or single
client.

Considering the high rate of concurrency, the ideal datastore should allow multiple
connections like a database. Reducing the need for external services/libraries this
service uses a datastore object with lock protection around the data structures.

The packages to be indexed are expected to have a single (unique) name and a set or
list of packages that must exist within the store; these packages cannot be added
until all dependencies are indexed and a package cannot be removed until all indexed
packages that depend on it are removed. To improve reverse lookup the datastore
will maintain a 'used by' list for each package to determine if the package is
eligible for removal without needing to walk through all the elements. If the datastore
was based on a database we could leverage one to many indexes instead of a copy of
package name.
"""
import threading
import socket
import logging
import traceback
from package_objects import DataPackageStore, DataPackage
from SocketServer import ThreadingMixIn, BaseRequestHandler, TCPServer
from Queue import Queue

# Test machine configuration
# VM with 4 cores, 8 GB memory, 100 GB harddrive
# Ubuntu 17.04 (16.04 tested as well), Python 2.7.13

# Note below regarding larger concurrency tests

# Previous attempt tracker
# 1) Single threaded TCP Server (timeouts)
#   - Works with test_client, but fails with do-package-tree_*
# 2) Thread spawner service (new thread per connection) (system resource limits)
# 3) Process spawner (asynchat library) to avoid GIL. Resource access issues
#   - multiprocessing.Manager could not gaurantee access between processes for complex objects
# 4) Threadpool Service (current).

# Tests:
# 1) Package parsing test (executed by python package_objects.py > Failure generates assert)
# 2) Service client connection (lightweight test for single and multiple clients through TCP)
# 3) Manual execution of telnet to localhost:8080 for pushing individual package strings.

logging.basicConfig(level=logging.INFO,
                    format="%(created)-15s %(msecs)d %(levelname)8s %(thread)d %(name)s %(message)s")
logger = logging.getLogger(__name__)

# Build the response strings once and store as global refs
RESPONSE_OK = "OK\n"
RESPONSE_FAIL = "FAIL\n"
RESPONSE_ERROR = "ERROR\n"

# Have a general reference to the address and port to use for the service.
# TODO - These could pull from a config file
HOST = ''
PORT = 8080

# Create a single instance of the package store for reference within the client processors
data_store = DataPackageStore()


class TCPClientHandler(BaseRequestHandler):
    """Custom TCP client connection handler to process the index requests."""
    def handle(self):
        try:
            logger.debug("New client connection established...")
            data = 1  # Use a generic value to enter the loop

            while data is not None:
                # self.request is the client connection
                data = self.request.recv(2048)  # Get 2K worth of data

                logger.debug("Client sent data [{}]".format(data))

                reply = self.process_request(data)

                logger.debug("Sending response data [{}]".format(reply))

                if reply is not None:
                    self.request.send(reply)
        except Exception:
            try:
                self.request.close()
            except:
                pass  # Ignore close exception as this means the connection was closed

    def process_request(self, data):
        # Assume things went well... positivity!
        response = RESPONSE_OK
        try:
            # Parse the message into a package instance
            req = DataPackage(data)
            if req.is_query():
                if not data_store.find_package(req.name) is not None:
                    logger.debug("Unable to find package {}".format(req.name))
                    response = RESPONSE_FAIL
            elif req.is_remove():
                pack = data_store.remove_package(req.name)
                if not pack:
                    logger.debug("Unable to remove package {}".format(req.name))
                    response = RESPONSE_FAIL
                # If the package wasn't found it's been "removed"
            else:
                # req.is_index()
                p = data_store.add_package(req)
                if not p:
                    logger.debug("Unable to index package {}".format(req.name))
                    response = RESPONSE_FAIL
        except ValueError:
            # value errors indicate invalid requests
            logger.debug("Invalid request data... [{}]".format(data))
            response = RESPONSE_ERROR
        except Exception as ex:
            # catch all case...
            logger.debug(traceback.format_exc())  # Only trace in debug
            logger.error("Unable to process data '{}'. {}:{}".format(data,
                                                                     type(ex),
                                                                     ex))
            response = RESPONSE_ERROR

        return response


class ThreadPoolMixIn(ThreadingMixIn):
    """Create a threadpool mix in to limit the number of threads running in parallel."""
    # NOTE: The number of threads is directly proportionaly to the number of clients this can handle
    # Initial test ran with 10 but failed on concurrency 50 and 100
    # Secondary test ran with 50 but failed on 100
    # Additional tests at 75, 80, and 90 returned periodic success indicating the timeout
    # was due to the fact that client requests would be blocked from the queue until the worker
    # thread completed processing a single client. The test clients did not gracefully handle
    # segmented work (client close after a single request was process) so this was increased to
    # accommodate. A client that could rebuild connection after remote close would allow us to
    # reduce this resource limit and share threads for individual connections.
    numThreads = 100
    # Allow the server address to be re-used so a service restart doesn't error
    allow_reuse_address = True

    def process_client_request(self):
        """Client processing method for an individual thread.

        This method runs an infinite loop for the thread until an external interruption.
        """
        while True:
            # Each thread will continue to pull requests from the queue
            ThreadingMixIn.process_request_thread(self, *self.requests.get())

    def serve_forever(self):
        """Continue to serve client requests until external interrupt (Ctrl-C)."""
        # Use a queue to hold all the client requests, limited to the thread max
        self.requests = Queue(self.numThreads)

        # Spawn the worker threads
        for _ in range(self.numThreads):
            t = threading.Thread(target=self.process_client_request)
            t.setDaemon(1)
            t.start()

        # Keep running the server until Ctrl-C
        while True:
            self.handle_request()

        # Close out the server process.
        self.server_close()

    def handle_request(self):
        """Pushes a client request and socket details on the queue for processing."""
        try:
            request, client_address = self.get_request()
        except socket.error:
            # If there's a problem getting the request... exit out and loop back.
            return
        # Verify the request is valid (ignore invalid requests)
        if self.verify_request(request, client_address):
            self.requests.put((request, client_address))


if __name__ == '__main__':

    class ThreadPoolServer(ThreadPoolMixIn, TCPServer):
        """ A custom TCP server that supports a thread pool for client connection management."""
        pass

    try:
        # Create an instance of the server
        server = ThreadPoolServer((HOST, PORT), TCPClientHandler)

        sa = server.socket.getsockname()
        logger.info("Starting service on {}:{}...".format(sa[0], sa[1]))
        logger.info("Press Ctrl-C to stop the service...")
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down service...")
    finally:
        data_store.print_state()
