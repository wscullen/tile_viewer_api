
import socket
import os
import sys
import platform
import psutil
import uuid


class SystemInfo():
    def __init__(self):
        self.hostname = socket.gethostname()
        self.sys_platform = sys.platform
        self.platform = platform.platform()
        self.cpu = platform.processor()
        self.os = platform.system()
        self.os_ver = platform.version()
        self.os_release = platform.release()
        self.cpu_cores = psutil.cpu_count(logical=False)

    def get_info_dict(self):
        return {
            'hostname': self.hostname,
            'system_platform': self.sys_platform,
            'platform': self.platform,
            'cpu': self.cpu,
            'os': self.os,
            'os_ver': self.os_ver,
            'os_rel': self.os_release,
            'cores': self.cpu_cores,
        }