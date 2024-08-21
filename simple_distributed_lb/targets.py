import ipaddress
import threading
from dataclasses import dataclass
from itertools import cycle
from typing import Dict, List

import pydantic


@dataclass
class LoadBalancerTarget:
    ip_address: str
    port: int

    def __init__(self, ip_address, port):
        if ipaddress.ip_address(ip_address):
            self.ip_address = ip_address
        else:
            raise ValueError("Invalid IP address")
        self.port = int(port)
        super().__init__()

    def __hash__(self):
        return hash((self.ip_address, self.port))


class LoadBalancerTargets:
    def __init__(self, *targets_list: LoadBalancerTarget):
        self.targets: List[LoadBalancerTarget] = list(targets_list)
        self.current_index = -1

    def add(self, target: LoadBalancerTarget):
        if target in self.targets:
            raise ValueError("Target already exists")
        self.targets.append(target)

    def reset_round_robin(self):
        self.current_index = -1

    def __len__(self):
        return len(self.targets)

    def get_next_target(self):
        if len(self.targets) == 0:
            raise ValueError("No targets registered")
        if self.current_index >= len(self.targets):
            self.reset_round_robin()
        self.current_index += 1
        return self.targets[self.current_index]


class ThreadsafeLoadBalancerTargets(LoadBalancerTargets):
    def __init__(self, *targets_list: LoadBalancerTarget):
        self.lock = threading.Lock()
        super().__init__(*targets_list)

    def add(self, target: LoadBalancerTarget):
        with self.lock:
            super().add(target)

    def reset_round_robin(self):
        with self.lock:
            super().reset_round_robin()

    def get_next_target(self):
        with self.lock:
            return super().get_next_target()


class Host(pydantic.BaseModel):
    ip_address: str
    port: int

    def __str__(self):
        return f"{self.ip_address}:{self.port}"

    def __hash__(self):
        return hash((self.ip_address.lower(), self.port))


class UpstreamTarget(Host):
    path: str = "/"

    def __str__(self):
        host_str = super().__str__()
        return f"{host_str}:{self.path}"

    def __hash__(self):
        return hash((self.ip_address.lower(), self.port, self.path.lower()))


class RoundRobinTargets:
    def __init__(self, targets: List[Host] = None):
        self._targets = {}
        for target in targets or []:
            self.add(target)
        self._cycles = {}

    @property
    def targets(self) -> Dict[str, Host]:
        return self._targets

    def _reset_cycle(self):
        self._cycle = cycle(self._targets)

    def get_next(self, path: str):
        if path not in self._targets:
            raise ValueError(f"No targets registered for path: {path}")
        path_cycle = self._cycles.setdefault(path, cycle(self._targets[path]))
        next_item = next(path_cycle)
        return next_item

    def add(self, path: str, target: Host) -> bool:
        per_path_targets = self._targets.setdefault(path, [])
        if target not in per_path_targets:
            per_path_targets.append(target)
            return True
        return False

    def __repr__(self):
        return f"{self.__class__.__name__}({self._targets})"
