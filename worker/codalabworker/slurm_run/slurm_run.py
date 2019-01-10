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

from codalabworker.formatting import size_str
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
    parser.add_argument('--state-file-path')
    return parser.parse_args()


def main():
    args = parse_args()
    with open(args.bundle_file, 'r') as infile:
        bundle = pickle.load(infile)
    with open(args.resources_file, 'r') as infile:
        resources = pickle.load(infile)
    slurm_run = SlurmRun(bundle, resources, args.state_file_path)
    slurm_run.start()


DependencyKey = namedtuple('DependencyKey', 'parent_uuid parent_path')


class SlurmRun(object):
    def __init__(self, bundle, resources, comm_file_path):
        self.bundle = bundle  # type: BundleInfo
        self.resources = resources  # type: RunResources
        self.docker_client = docker.from_env()

        self.state_file_path = comm_file_path + '.state'
        self.state_file_write_thread = threading.Thread(target=self.write_state, args=[])
        self.state_file_lock_path = self.state_file_path + '.lock'

        self.command_file_path = comm_file_path + '.commands'
        self.command_file_read_thread = threading.Thread(target=self.read_commands, args=[])
        self.commands_lock = threading.RLock()
        self.command_file_lock_path = self.command_file_path + '.lock'

        self.docker_image_download_thread = None
        self.docker_image_state = DependencyState(status='Waiting for docker image', stage=DependencyStage.DOWNLOADING)

        self.dependency_paths = set()  # type Set[str]
        self.dependency_download_threads = ThreadDict(fields={'success': False, 'status': None})

        self.run_state_lock = threading.RLock()
        self.run_state = WorkerRun(
            uuid=bundle['uuid'],
            run_status='',
            start_time=time.time(),
            docker_image='',
            info={},
            state=State.PREPARING,
        )

        self.killed = False
        self.running = True

    def write_state(self):
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

    def apply_commands(self):
        """
        Run commands like:
            - kill
            - read
            - write
            - netcat
        """
        pass

    def start(self):
        self.state_file_write_thread.start()
        self.command_file_read_thread.start()
        try:
            self.prepare_dependencies()
            if not self.killed:
                self.run_container()
        except Exception as ex:
            # TODO: failure case
            pass
        self.finalize()

    def assign_path(self, dependency):
        """
        Returns the path for the given dependency. Should be the mountpoint for the run bundle container
        """
        pass

    def downloading_dependencies(self):
        return (self.docker_image_download_thread and self.docker_image_download_thread.is_alive()) or any(thread.is_alive() for thread in self.dependency_download_threads.values())

    def prepare_dependencies(self):
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
                        self.preparation_state.docker_image_state = DependencyState(status='Docker image download failed: %s' % ex, stage=DependencyStage.FAILED)
            # Tag the image to save that this bundle is dependent on this image
            image.tag('codalab-image-cache/dependents', tag=self.bundle.uuid)
            # Timestamp the image using a tag, removing the old timestamp if there is one
            timestamp = str(time.time())
            for tag in image.tags:
                if tag.split(':')[0] == 'codalab-image-cache/last-used':
                    self.docker_client.images.remove(tag)
            image.tag('codalab-image-cache/last-used', tag=timestamp)
            with self.run_state_lock:
                self.docker_image_state = DependencyState(status='Docker image ready', stage=DependencyStage.READY)
                # Save the digest to bundle state to report back to the server
                self.run_state.docker_image = image.attrs.get('RepoDigests', [self.resources.docker_image])[0]

        self.docker_image_download_thread = threading.Thread(target=download_docker_image, args=[])
        self.docker_image_download_thread.start()

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
                        self.dependency_download_threads[dependency_key].status = "Downloading dependency %s: %s downloaded" % (dependency.parent_name, size_str(bytes_downloaded[0]))
                        if self.killed:
                            raise DownloadAbortedException("Bundle killed")
                        return data

                    fileobj.read = interruptable_read

                    # Start copying the fileobj to filesystem dependency path
                    store_dependency(path, fileobj, target_type)
                self._downloading[dependency]['success'] = True
                self._downloading[dependency][
                    'status'
                ] = "Dependency %s downloaded: %s " % dependency.parent_name
            except Exception as e:
                self._downloading[dependency]['success'] = False
                self._downloading[dependency][
                    'status'
                ] = "Dependency %s failed: %s " % (dependency.parent_name, str(e))

        for dep in self.bundle.dependencies:
            dependency_key = DependencyKey(parent_uuid=dep.parent_uuid, parent_path=dep.parent_path)
            path = self.assign_path(dep)
            with self.run_state_lock:
                self.dependency_download_threads.add_if_new(dependency_key, threading.Thread(target=download_dependency, args=[dependency_key, dep, path]))
        # Wait for all downloads to finish or fail
        while self.downloading_dependencies():
            self.update_preparation_status()
            time.sleep(UPDATE_INTERVAL)
        self.docker_image_download_thread.join()
        for thread in self.dependency_download_threads.values():
            thread.join()
        if not all(dep['success'] for dep in self.dependency_download_threads.values()) or self.docker_image_state.stage == DependencyStage.FAILED:
            dependency_statuses = ', '.join(thread['status'] for thread in self.dependency_download_threads.values())
            with self.run_state_lock:
                docker_image_status = self.docker_image_state.status
            raise PreparationFailedException("Preparation failed: %s. %s" % (docker_image_status, dependency_statuses))
        with self.run_state_lock:
            self.run_state.run_status = "Docker image and dependency downloads complete"

    def update_preparation_status(self):
        dependency_statuses = ', '.join(thread['status'] for thread in self.dependency_download_threads.values())
        with self.run_state_lock:
            docker_image_status = self.docker_image_state.status
            self.run_state.run_status = "Preparing dependencies: %s. %s" % (docker_image_status, dependency_statuses)
