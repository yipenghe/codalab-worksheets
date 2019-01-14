import argparse
from collections import namedtuple
from contextlib import closing
import docker
import errno
import fcntl
import os
import pickle
import threading
import time
import traceback

from codalabworker.file_util import remove_path, get_path_size
from codalabworker import docker_utils
from codalabworker.formatting import size_str, duration_str
from codalabworker.fsm import DependencyState, DependencyStage
from codalabworker.local_run.local_dependency_manager import store_dependency
from codalabworker.bundle_state import State, WorkerRun, RunResources, BundleInfo
from codalabworker.worker_thread import ThreadDict


# Interval (in seconds) to update the state file
UPDATE_INTERVAL = 5


class DownloadAbortedException(Exception):
    """
    Exception raised by the download if a download is killed before it is complete
    """

    def __init__(self, message):
        super(DownloadAbortedException, self).__init__(message)


class PreparationFailedException(Exception):
    """
    Exception raised by dependency preparation if docker image or dependency downloads
    don't complete successfully
    """

    def __init__(self, message):
        super(DownloadAbortedException, self).__init__(message)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bundle-file')
    parser.add_argument('--resources-file')
    parser.add_argument('--run-info-file')
    parser.add_argument('--comm-file-path')
    return parser.parse_args()


def main():
    args = parse_args()
    with open(args.bundle_file, 'r') as infile:
        bundle = pickle.load(infile)
    with open(args.resources_file, 'r') as infile:
        resources = pickle.load(infile)
    with open(args.run_info_file, 'r') as infile:
        run_info = pickle.load(infile)
    slurm_run = SlurmRun(bundle, resources, run_info, args.comm_file_path)
    slurm_run.start()


DependencyKey = namedtuple('DependencyKey', 'parent_uuid parent_path')


class SlurmRunInfo(object):
    """
    Object to store Slurm-specific information that the run manager needs to pass to the Run script
    :param bundle_path: Absolute path to where the bundle contents should be stored on the worker machine
    :param dependencies_path: Absolute path to where the dependencies should be downloaded
    :param docker_network: Docker network to use for this bundle (internal/external)
    :param docker_runtime: Docker runtime to use for this bundle (nvidia/runc)
    """

    def __init__(
        self,
        bundle_path,
        dependencies_path,
        docker_network,
        docker_runtime,
    ):
        self.bundle_path = bundle_path
        self.dependencies_path = dependencies_path
        self.docker_network = docker_network
        self.docker_runtime = docker_runtime


class SlurmRun(object):
    def __init__(self, bundle, resources, run_info, comm_file_path):
        self.bundle = bundle  # type: BundleInfo
        self.resources = resources  # type: RunResources
        self.run_info = run_info  # type: SlurmRunInfo
        self.docker_client = docker.from_env()

        self.dependency_paths = {}  # type: Dict[DependencyKey, str]

        self.state_file_path = comm_file_path + '.state'
        self.state_file_write_thread = threading.Thread(target=self.write_state, args=[])
        self.state_file_lock_path = self.state_file_path + '.lock'

        self.command_file_path = comm_file_path + '.commands'
        self.command_file_read_thread = threading.Thread(target=self.read_commands, args=[])
        self.command_file_lock_path = self.command_file_path + '.lock'

        self.docker_image_download_thread = None
        self.docker_image_state = DependencyState(
            status='Waiting for docker image', stage=DependencyStage.DOWNLOADING
        )

        self.dependency_download_threads = ThreadDict(fields={'success': False, 'status': None})

        self.run_state_lock = threading.RLock()
        self.commands_lock = threading.RLock()
        self.run_state = WorkerRun(
            uuid=bundle['uuid'],
            run_status='',
            start_time=time.time(),
            docker_image='',
            info={},
            state=State.PREPARING,
        )

        self.killed = False
        self.has_contents = False
        self.running = True

    def start(self):
        self.state_file_write_thread.start()
        self.command_file_read_thread.start()
        try:
            self.prepare_requirements()
            if not self.killed:
                container = self.run_container()
                self.has_contents = True
                self.monitor_container(container)
        except Exception as ex:
            # TODO: failure case
            pass
        self.cleanup()
        self.finalize_bundle()

    def cleanup(self):
        """
        Cleans up the downloaded dependencies, docker image and the container
        """
        pass

    def finalize_bundle():
        """
        Tells the RunManager that this bundle is complete and that the results can now be uploaded
        and the bundle finalized
        """
        pass

    def apply_commands(self):
        """
        Run commands like:
            - kill
            - read
            - write
            - netcat
            - finalize
        """
        pass

    def assign_path(self, dependency):
        """
        Returns the path where the given dependency should be stored.
        All dependencies are stored in run_info.dependencies_path
        """
        if dependency.parent_path:
            path = os.path.join(dependency.parent_uuid, dependency.parent_path)
        else:
            path = dependency.parent_uuid
        path = path.replace(os.path.sep, '_')
        path = os.path.join(self.run_info.dependencies_path, path)

        # You could have a conflict between, for example a/b_c and
        # a_b/c. We have to avoid those.
        while path in self.dependency_paths.values():
            path = path + '_'
        return path

    def preparing_requirements(self):
        return (
            self.docker_image_download_thread and self.docker_image_download_thread.is_alive()
        ) or any(thread.is_alive() for thread in self.dependency_download_threads.values())

    def get_docker_image(self):
        """
        Starts the docker image download on a separate thread.
        Tags the image with:
        - codalab-image-cache/dependents:<uuid>
        - codalab-image-cache/last-used:<timestamp-of-now>
        Removes previous timestamp tag if there is one
        """
        def download_docker_image():
            try:
                image = self.docker_client.images.get(self.resources.docker_image)
            except docker.errors.ImageNotFound:
                try:
                    with self.run_state_lock:
                        self.docker_image_state.status = 'Starting docker image download'
                    self.docker_client.images.pull(self.resources.docker_image)
                    image = self.docker_client.images.get(self.resources.docker_image)
                except (docker.errors.APIError, docker.errors.ImageNotFound) as ex:
                    with self.run_state_lock:
                        self.preparation_state.docker_image_state = DependencyState(
                            status='Docker image download failed: %s' % ex,
                            stage=DependencyStage.FAILED,
                        )
            # Tag the image to save that this bundle is dependent on this image
            image.tag('codalab-image-cache/dependents', tag=self.bundle.uuid)
            # Timestamp the image using a tag, removing the old timestamp if there is one
            timestamp = str(time.time())
            for tag in image.tags:
                if tag.split(':')[0] == 'codalab-image-cache/last-used':
                    self.docker_client.images.remove(tag)
            image.tag('codalab-image-cache/last-used', tag=timestamp)
            with self.run_state_lock:
                self.docker_image_state = DependencyState(
                    status='Docker image ready', stage=DependencyStage.READY
                )
                # Save the digest to bundle state to report back to the server
                self.run_state.docker_image = image.attrs.get(
                    'RepoDigests', [self.resources.docker_image]
                )[0]

        self.docker_image_download_thread = threading.Thread(target=download_docker_image, args=[])
        self.docker_image_download_thread.start()

    def get_bundle_dependencies(self):
        """
        Downloads all the dependencies needed for a run in paralllel
        """
        def download_dependency(dependency_key, dependency, path):
            try:
                # Start async download to the fileobj
                fileobj, target_type = self.bundle_service.get_bundle_contents(
                    dependency.parent_uuid, dependency.parent_path
                )
                with closing(fileobj):
                    # "Bug" the fileobj's read function so that we can keep
                    # track of the number of bytes downloaded so far.
                    old_read_method = fileobj.read
                    bytes_downloaded = [0]

                    def interruptable_read(*args, **kwargs):
                        data = old_read_method(*args, **kwargs)
                        bytes_downloaded[0] += len(data)
                        self.dependency_download_threads[dependency_key].status = (
                            "Downloading dependency %s: %s downloaded"
                            % (dependency.parent_name, size_str(bytes_downloaded[0]))
                        )
                        if self.killed:
                            raise DownloadAbortedException("Bundle killed")
                        return data

                    fileobj.read = interruptable_read

                    # Start copying the fileobj to filesystem dependency path
                    store_dependency(path, fileobj, target_type)
                self._downloading[dependency]['success'] = True
                self._downloading[dependency]['status'] = (
                    "Dependency %s downloaded: %s " % dependency.parent_name
                )
            except Exception as e:
                self._downloading[dependency]['success'] = False
                self._downloading[dependency]['status'] = "Dependency %s failed: %s " % (
                    dependency.parent_name,
                    str(e),
                )

        for dep_key, dep in self.bundle.dependencies.items():
            path = self.assign_path(dep)
            self.dependency_paths[dep_key] = path
            with self.run_state_lock:
                self.dependency_download_threads.add_if_new(
                    dep_key, threading.Thread(target=download_dependency, args=[dep_key, dep, path])
                )

    def prepare_requirements(self):
        """
        Downloads the docker image and dependencies for the run
        """
        self.get_docker_image()
        self.get_bundle_dependencies()
        # Wait for all downloads to finish or fail
        while self.preparing_requirements():
            self.update_preparation_status()
            time.sleep(UPDATE_INTERVAL)
        self.docker_image_download_thread.join()
        for thread in self.dependency_download_threads.values():
            thread.join()
        if (
            not all(dep['success'] for dep in self.dependency_download_threads.values())
            or self.docker_image_state.stage == DependencyStage.FAILED
        ):
            dependency_statuses = ', '.join(
                thread['status'] for thread in self.dependency_download_threads.values()
            )
            with self.run_state_lock:
                docker_image_status = self.docker_image_state.status
            raise PreparationFailedException(
                "Preparation failed: %s. %s" % (docker_image_status, dependency_statuses)
            )
        with self.run_state_lock:
            self.run_state.run_status = "Docker image and dependency downloads complete"

    def update_preparation_status(self):
        """
        Updates self.run_state.run_status with a message combined of all the download
        statuses of the dependencies and the docker image
        """
        dependency_statuses = ', '.join(
            thread['status'] for thread in self.dependency_download_threads.values()
        )
        with self.run_state_lock:
            docker_image_status = self.docker_image_state.status
            self.run_state.run_status = "Preparing bundle requirements: %s. %s" % (
                docker_image_status,
                dependency_statuses,
            )

    def start_container(self):
        """
        Prepares the filesystem for mountpoints and starts the container for the run
        :returns: a Docker Container object for the container just started for the run
        """
        remove_path(self.run_info.bundle_path)
        os.mkdir(self.run_info.bundle_path)
        docker_dependencies = []
        docker_dependencies_path = '/' + self.bundle.uuid + '_dependencies'
        for dep_key, dep in self.bundle.dependencies.items():
            child_path = os.path.normpath(os.path.join(self.run_info.bundle_path, dep.child_path))
            if not child_path.startswith(self.run_info.bundle_path):
                raise Exception('Invalid key for dependency: %s' % (dep['child_path']))

            docker_dependency_path = os.path.join(docker_dependencies_path, dep.child_path)
            os.symlink(docker_dependency_path, child_path)
            # These are turned into docker volume bindings like:
            #   dependency_path:docker_dependency_path:ro
            docker_dependencies.append((self.dependency_paths[dep_key], docker_dependency_path))

        container = docker_utils.start_bundle_container(
            self.run_info.bundle_path,
            self.bundle.uuid,
            docker_dependencies,
            self.bundle.command,
            self.resources.docker_image,
            network=self.docker_network,
            memory_bytes=self.resources.memory,
            runtime=self.run_info.docker_runtime,
        )
        return container

    def monitor_container(self, container):
        """
        Monitors the Docker container for the run, checking resource usage and killing it
        if it uses too many resources
        :param container: A Docker Container object for the run
        """
        def check_disk_utilization():
            while not self.run_state.info['finished']:
                start_time = time.time()
                try:
                    disk_utilization = get_path_size(self.bundle.path)
                    with self.run_state_lock:
                        self.run_state.info['disk_utilization'] = disk_utilization
                except Exception:
                    traceback.print_exc()
                end_time = time.time()

                # To ensure that we don't hammer the disk for this computation when
                # there are lots of files, we run it at most 10% of the time.
                time.sleep(max((end_time - start_time) * 10, 1.0))
        self.disk_utilization_thread = threading.Thread(target=check_disk_utilization, args=[])
        self.disk_utilization_thread.start()

        def check_and_report_finished():
            """
            Checks whether the container is finished and returns its exit code and failure message(if there is one) 
            if the container is finished
            :returns: Tuple[bool, Optional[str], Optional[str]]: finished, exit code, failure message if failure happened
            """
            try:
                finished, exitcode, failure_message = docker_utils.check_finished(container)
            except docker_utils.DockerException:
                finished, exitcode, failure_message = False, None, None
            new_info = dict(finished=finished, exitcode=exitcode, failure_message=failure_message)
            self.run_state.info.update(new_info)
            return finished, exitcode, failure_message

        def check_resource_utilization():
            """.
            Checks the time, memory and disk use of the container, setting it up to be killed
            if it is going over its allocated resources
            :returns: List[str]: Kill messages with reasons to kill if there are reasons, otherwise empty
            """
            kill_messages = []

            run_stats = docker_utils.get_container_stats(container)
            time_used = time.time() - self.run_state.start_time

            self.run_state.info.update(dict(
                time_used=time_used, 
                max_memory=max(self.run_state.info['max_memory'], run_stats.get('memory', 0))
            ))

            if (
                self.resources.request_time
                and self.run_state.info['time_used'] > self.resources.request_time
            ):
                kill_messages.append(
                    'Time limit %s exceeded.' % duration_str(self.resources.request_time)
                )

            if self.run_state.info['max_memory'] > self.resources.request_memory:
                kill_messages.append(
                    'Memory limit %s exceeded.'
                    % duration_str(self.resources.request_memory)
                )

            if (
                self.resources.request_disk
                and self.run_state.info['disk_utilization'] > self.resources.request_disk
            ):
                kill_messages.append(
                    'Disk limit %sb exceeded.' % size_str(self.resources.request_disk)
                )

            return kill_messages

        finished = False
        while not self.killed and not finished:
            time.sleep(UPDATE_INTERVAL)
            finished, exitcode, failure_message = self.check_and_report_finished()
            kill_messages = self.check_resource_utilization()
            if kill_messages:
                self.killed = True

            if self.killed:
                try:
                    container.kill()
                except docker.errors.APIError:
                    finished, _, _ = docker_utils.check_finished(container)
                    if not finished:
                        # If we can't kill a Running container, something is wrong
                        # Otherwise all well
                        traceback.print_exc()
        self.disk_utilization_thread.join()

    def write_state(self):
        """
        Writes the state of the run to the shared state Pickle file
        every UPDATE_INTERVAL seconds
        """
        while self.running:
            while True:
                try:
                    lock_file = open(self.state_file_lock_path, 'w+')
                    fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except IOError as ex:
                    if ex.errno != errno.EAGAIN:
                        raise
                    else:
                        time.sleep(0.1)
            with open(self.state_file_path, 'wb') as f:
                with self.run_state_lock:
                    pickle.dump(self.run_state, f)
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()
        time.sleep(UPDATE_INTERVAL)

    def read_commands(self):
        """
        Reads bundle commands written by the worker to the shared commands file
        every UPDATE_INTERVAL seconds
        """
        while self.running:
            while True:
                try:
                    lock_file = open(self.command_file_lock_path, 'w+')
                    fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except IOError as ex:
                    if ex.errno != errno.EAGAIN:
                        raise
                    else:
                        time.sleep(0.1)
            with open(self.command_file_path, 'wrb') as f:
                with self.commands_lock:
                    self.commands = pickle.load(f)
            fcntl.flock(lock_file, fcntl.LOCK_UN)
            lock_file.close()
            self.apply_commands()
        time.sleep(UPDATE_INTERVAL)
