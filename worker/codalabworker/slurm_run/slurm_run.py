import argparse
import docker
import errno
import fcntl
import pickle
import threading
import time

from codalabworker.fsm import DependencyStage
from codalabworker.bundle_state import State


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


class SlurmRun(object):

    def __init__(self, bundle, resources, comm_file_path):
        self.bundle = bundle
        self.resource = resources
        self.docker = docker.from_env()
        self.state_file_path = comm_file_path + '.state'
        self.state_file_lock_path = self.state_file_path + '.lock'
        self.command_file_path = comm_file_path + '.commands'
        self.command_file_lock_path = self.command_file_path + '.lock'

    def write_state(self):
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
            pickle.dump(self.run_state, f)
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()

    def read_commands(self):
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
        with open(self.command_file_path, 'rb') as f:
            self.commands = pickle.load(f)
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        lock_file.close()

    def start(self):
        try:
            self.prepare_dependencies()
            self.run_container()
        except Exception as ex:
            # TODO: failure case
            pass
        self.finalize()
