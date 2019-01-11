class DependencyKey(object):
    """
    Defines the uniquely identifying properties of a dependency that can be used as a key
    for caching dependencies
    """

    def __init__(self, parent_uuid, parent_path):
        self.parent_uuid = parent_uuid
        self.parent_path = parent_path

    def __eq__(self, other):
        return self.parent_uuid == other.parent_uuid and self.parent_path == other.parent_path

    def __hash__(self):
        return hash((self.parent_uuid, self.parent_path))


class Dependency(object):
    """
    Defines a RunBundle dependency passed from server to worker.
    Refer to codaalab/lib/bundle_util.py#bundle_to_bundle_info() for how the
    dict to construct this object is created on server side.
    """

    def __init__(self, parent_name, parent_path, parent_uuid, child_path, child_uuid):
        self.parent_name = parent_name
        self.parent_path = parent_path
        self.parent_uuid = parent_uuid
        self.child_path = child_path
        self.child_uuid = child_uuid


class BundleInfo(object):
    """
    Defines the bundle info passed to the worker by the server.
    Refer to codaalab/lib/bundle_util.py#bundle_to_bundle_info() for how the
    dict to construct this object is created on server side.
    """

    def __init__(
        self,
        uuid,
        bundle_type,
        owner_id,
        command,
        data_hash,
        state,
        is_anonymous,
        metadata,
        dependencies,  # List[str, Dict[str, str]]
        args,
    ):
        self.uuid = uuid  # type: str
        self.bundle_type = bundle_type  # type: str
        self.owner_id = owner_id  # type: str
        self.command = command  # type: str
        self.data_hash = data_hash  # type: str
        self.state = state  # type: State
        self.is_anonymous = is_anonymous  # type: bool
        self.metadata = metadata  # type: Dict[Any, Any]
        self.args = args  # type: Any
        self.dependencies = {
            DependencyKey(dep['parent_uuid'], dep['parent_path']): Dependency(
                parent_name=dep['parent_name'],
                parent_path=dep['parent_path'],
                parent_uuid=dep['parent_uuid'],
                child_path=dep['child_path'],
                child_uuid=dep['child_uuid'],
            )
            for dep in dependencies
        }  # type: Dict[DependencyKey, Dependency]

        @classmethod
        def from_dict(cls, dct):
            return cls(
                uuid=dct['uuid'],
                bundle_type=dct['bundle_type'],
                owner_id=dct['owner_id'],
                command=dct['command'],
                data_hash=dct['data_hash'],
                state=dct['state'],
                is_anonymous=dct['is_anonymous'],
                metadata=dct['metadata'],
                dependencies=dct['dependencies'],
                args=dct['args'],
            )


class RunResources(object):
    """
    Defines all the resource fields the server propagates to the worker for its runs
    """

    def __init__(self, cpus, gpus, docker_image, time, memory, disk, network):
        self.cpus = cpus  # type: int
        self.gpus = gpus  # type: int
        self.docker_image = docker_image  # type: str
        self.time = time  # type: int
        self.memory = memory  # type: int
        self.disk = disk  # type: int
        self.network = network  # type: bool

        if ':' not in self.docker_image:
            self.docker_image += ':latest'

    @classmethod
    def from_dict(cls, dct):
        return cls(
            cpus=int(dct['cpus']),
            gpus=int(dct['gpus']),
            docker_image=dct['docker_image'],
            time=int(dct['time']),
            memory=int(dct['memory']),
            disk=int(dct['disk']),
            network=bool(dct['network']),
        )


class WorkerRun(object):
    """
    Defines all the field the worker needs to check in with the server for its runs
    """

    def __init__(self, uuid, run_status, start_time, docker_image, info, state):
        self.uuid = uuid
        self.run_status = run_status
        self.start_time = start_time
        self.docker_image = docker_image
        self.info = info
        self.state = state


class State(object):
    """
    An enumeration of states that a bundle can be in.
    """

    # Waiting for contents to be uploaded
    UPLOADING = 'uploading'
    # Just created
    CREATED = 'created'
    # All the dependencies are met
    STAGED = 'staged'
    # Creating a make bundle.
    MAKING = 'making'
    # Waiting for the worker to start up.
    WAITING_FOR_WORKER_STARTUP = 'waiting_for_worker_startup'
    # Wait for the worker to start running the bundle.
    STARTING = 'starting'
    # Wait for worker to download dependencies and docker images
    PREPARING = 'preparing'
    # Actually running
    RUNNING = 'running'
    # Run finished and finalized server-side, tell worker to discard it
    FINALIZING = 'finalizing'
    # Done running and succeeded
    READY = 'ready'
    # Done running and failed
    FAILED = 'failed'
    # Killed by user
    KILLED = 'killed'
    # Assigned worker has gone offline
    WORKER_OFFLINE = 'worker_offline'

    OPTIONS = {
        CREATED,
        STAGED,
        MAKING,
        WAITING_FOR_WORKER_STARTUP,
        STARTING,
        RUNNING,
        READY,
        FAILED,
        PREPARING,
        FINALIZING,
    }
    ACTIVE_STATES = {MAKING, WAITING_FOR_WORKER_STARTUP, STARTING, RUNNING, FINALIZING, PREPARING}
    FINAL_STATES = {READY, FAILED, KILLED}
