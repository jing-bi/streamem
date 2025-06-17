from posix_ipc import (
    Semaphore,
    O_CREX,
    unlink_shared_memory,
    ExistentialError,
    O_CREAT,
    SharedMemory,
)
import mmap
import numpy as np
import time
import threading
import json
import queue
import platform

system = platform.system()


def numbers_sum(tot, n):
    if tot <= 0 or n <= 0:
        return []
    number = tot // n
    remainder = tot % n
    return [number + 1] * remainder + [number] * (n - remainder)


if system == "Darwin":
    import socket

    message_queue = queue.Queue()

    def handle_client(client_socket, address):
        while True:
            message = client_socket.recv(1024).decode()
            if not message:
                break
            message_queue.put(message)
        client_socket.close()

    class CommMech:
        def __init__(self, name, mode="w"):
            self.q_name = f"/{name}"
            self.mode = mode
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_address = (
                "localhost",
                12345,
            )  # Update with appropriate server address
            if mode == "w":
                self.socket.bind(self.server_address)
                self.socket.listen(8)
                threading.Thread(target=self._server_handler).start()

            if mode == "r":
                try:
                    self.connection = self.socket
                    self.connection.connect(self.server_address)
                    # self.socket.sendall(b'UNLINK' + self.q_name.encode())
                except ConnectionRefusedError:
                    pass

        def _server_handler(self):
            while True:
                connection, addr = self.socket.accept()
                client_thread = threading.Thread(
                    target=handle_client, args=(connection, addr)
                )
                client_thread.start()

        def recv(self):
            return json.loads(message_queue.get())

        def send(self, msg):
            self.connection.sendall(json.dumps(msg).encode())

        def __del__(self):
            self.socket.close()

else:
    from posix_ipc import MessageQueue, unlink_message_queue

    class CommMech:
        def __init__(self, name, mode="w"):
            self.q_name = f"/{name}"
            self.mode = mode
            if mode == "w":
                try:
                    unlink_message_queue(self.q_name)
                except:
                    pass
                self.mq = MessageQueue(self.q_name, max_messages=1, flags=O_CREAT)
            if mode == "r":
                self.mq = MessageQueue(self.q_name)

        def recv(self):
            data = json.loads(self.mq.receive()[0])
            return data

        def send(self, msg):
            self.mq.send(json.dumps(msg))

        def __del__(self):
            if self.mode == "w":
                self.mq.unlink()


class SharedFrame:
    def __init__(self, name, shape=None, mode="r", dtype="uint8"):
        self.name = name
        self.mode = mode
        self.sems = {}
        self.sems_lock = threading.Lock()  # Add lock for thread-safe access to sems
        self.somthing = []
        self.meta = ["ram", "stm", "cnt", "mat"]
        for res in self.meta:
            setattr(self, f"{res}_name", f"{res}-{name}")
            setattr(self, f"{res}_sem_name", f"{res}-sem-{name}")
        if self.mode == "w":
            self.dtype = dtype
            self.ram = self._crea_mem(
                self.ram_name, int(np.prod(shape) * getattr(np, dtype)().nbytes)
            )
            self.stm = self._crea_mem(self.stm_name, np.int64().nbytes)
            self.cnt = self._crea_mem(self.cnt_name, np.int32().nbytes)
            self.mat = self._crea_mem(self.mat_name, 40)
            for res in self.meta:
                setattr(
                    self, f"{res}_sem", self._crea_sem(getattr(self, f"{res}_sem_name"))
                )
            self._serialize(shape, dtype)

            self.comm = CommMech(self.name, mode="w")

            threading.Thread(target=self.dsync).start()
            print(f"New {self.ram_name} {shape}")

        if self.mode == "r":
            for res in self.meta:
                setattr(self, f"{res}", self._crex_mem(getattr(self, f"{res}_name")))
                setattr(
                    self, f"{res}_sem", self._crex_sem(getattr(self, f"{res}_sem_name"))
                )
            self.shape, self.dtype = self._deserialize()
            self.comm = CommMech(self.name, mode="r")
            print(f"Get {self.ram_name} {self.shape}")

    def _serialize(self, shape, dtype):
        self.mat_sem.acquire()
        size = self.mat.shape[0]
        fill_up_size = numbers_sum(size, 2)
        self.mat[:] = bytes(
            f"{'x'.join(map(str, shape)).ljust(fill_up_size[0],'*')}|{dtype.ljust(fill_up_size[-1]-1,'*')}",
            "utf-8",
        )
        self.mat_sem.release()

    def _deserialize(self):
        self.mat_sem.acquire()
        self.shape, self.dtype = (
            self.mat.tobytes().decode("utf-8").replace("*", "").split("|")
        )
        self.shape = list(map(int, self.shape.split("x")))
        self.dtype = getattr(np, self.dtype)
        self.mat_sem.release()
        return self.shape, self.dtype

    def dsync(self):
        while True:
            msg = self.comm.recv()
            if msg["command"] == "signin":
                self.signin(msg["r_id"])
            if msg["command"] == "signout":
                self.signout(msg["r_id"])

    def signin(self, r_id):
        print(f"{self.name}: {r_id} signin")
        with self.sems_lock:
            if self.mode == "r":
                self.comm.send({"r_id": r_id, "command": "signin"})
                self.sems[r_id] = self._crex_sem(f"sem-{self.name}-{r_id}")
            elif self.mode == "w":
                self.sems[r_id] = self._crea_sem(f"sem-{self.name}-{r_id}")

    def signout(self, r_name):
        try:
            print(f"{self.name}: {r_name} signout")

            with self.sems_lock:
                if r_name in self.sems:
                    self.sems[r_name].unlink()
                    self.comm.send({"r_id": r_name, "command": "signout"})
                    del self.sems[r_name]
        except:
            pass

    def _crea_sem(self, name):
        try:
            sem = Semaphore(name, O_CREX)
        except ExistentialError:
            sem = Semaphore(name, O_CREAT)
            sem.unlink()
            sem = Semaphore(name, O_CREX)
        sem.release()
        return sem

    def _crex_sem(self, name):
        sem = None
        while not sem:
            try:
                sem = Semaphore(name)
            except ExistentialError:
                print(f"Waiting for [{name}] is available.")
                time.sleep(0.4)
        return sem

    def _crea_mem(self, name, nbytes):
        try:
            ref = SharedMemory(name, O_CREX, size=nbytes)
        except ExistentialError:
            unlink_shared_memory(name)
            # ref = SharedMemory(name, O_CREAT, size=nbytes)
            # ref.unlink()
            ref = SharedMemory(name, O_CREX, size=nbytes)
        buf = memoryview(mmap.mmap(ref.fd, ref.size))
        ref.close_fd()
        return buf

    def _crex_mem(self, name):
        ref = None
        while not ref:
            try:
                ref = SharedMemory(name=name)
            except ExistentialError:
                print(f"Waiting for [{name}] is available.")
                time.sleep(1)
        buf = memoryview(mmap.mmap(ref.fd, ref.size))
        ref.close_fd()
        return buf

    def write(self, data, stm):
        self.ram_sem.acquire()
        self.ram[:] = data if isinstance(data, bytearray) else data.tobytes()
        self.stm[:8] = stm.to_bytes(8, "little")
        self.ram_sem.release()
        with self.sems_lock:
            for k in list(self.sems.keys()):  # Create a copy of keys to avoid modification during iteration
                if k in self.sems:  # Double-check key still exists
                    if system != "Darwin":
                        if self.sems[k].value > 0:
                            continue
                    self.sems[k].release()

    def read(self, r_id):
        # Check if the semaphore still exists before trying to acquire it
        with self.sems_lock:
            if r_id not in self.sems:
                raise KeyError(f"Semaphore for reader {r_id} not found")
            sem = self.sems[r_id]
        
        sem.acquire()
        self.cnt_sem.acquire()
        cnt = np.ndarray(shape=(1,), dtype=np.int32, buffer=self.cnt)
        cnt += 1
        if cnt == 1:
            self.ram_sem.acquire()
        self.cnt_sem.release()

        data = np.ndarray(shape=self.shape, dtype=self.dtype, buffer=self.ram)
        stm = int.from_bytes(self.stm[:8], "little")
        self.cnt_sem.acquire()
        cnt = np.ndarray(shape=(1,), dtype=np.int32, buffer=self.cnt)
        cnt -= 1
        if cnt.item() == 0:
            self.ram_sem.release()
        self.cnt_sem.release()
        return (data.copy(), stm)
