from .memory import SharedFrame
from multiprocessing import Process
import time
import numpy as np
import platform

system = platform.system()


class Sensor(Process):
    def __init__(self) -> None:
        super(Sensor, self).__init__()
        self.running = True
        self.host = "192.168.1.80"

    def run(self):
        self.config()
        self.client.open()
        while self.running:
            data = self.client.get_next_packet()
            self.mem.write(data.payload, data.timestamp)

    def config(self):
        pass

    def terminate(self):
        self.running = False
        self.client.close()


class RandCamera(Sensor):
    def config(self):
        self.name = "randcam"
        self.width = 1280
        self.height = 720
        self.dtype = "uint8"
        self.mem = SharedFrame(
            self.name, (self.height, self.width, 3), mode="w", dtype=self.dtype
        )

    def run(self):
        self.config()
        while self.running:
            self.mem.write(
                np.random.randint(0, 255, size=(self.height, self.width, 3)).astype(
                    np.uint8
                ),
                0,
            )


class RandMicrophone(Sensor):
    def config(self):
        self.name = "randmc"
        self.shape = [2, 1024]
        self.dtype = "float32"
        self.mem = SharedFrame(self.name, self.shape, mode="w", dtype=self.dtype)

    def run(self):
        self.config()
        while self.running:
            self.mem.write(
                np.random.randint(0, 255, size=(self.height, self.width, 3)).astype(
                    np.uint8
                ),
                0,
            )
            time.sleep(0.2)


class RGBCamera(Sensor):
    def config(self):
        from . import hl2ss

        self.port = hl2ss.StreamPort.PERSONAL_VIDEO
        # Operating mode
        # 0: video
        # 1: video + camera pose
        # 2: query calibration (single transfer)
        self.mode = hl2ss.StreamMode.MODE_0
        self.name = "rgb"
        self.dtype = "uint8"
        # self.width = 1920
        self.width = 1280
        self.height = 720
        # self.height = 1080
        self.framerate = 30
        self.profile = hl2ss.VideoProfile.H265_MAIN
        self.bitrate = 5 * 1024 * 1024
        self.client = hl2ss.rx_pv(
            self.host,
            self.port,
            hl2ss.ChunkSize.PERSONAL_VIDEO,
            self.mode,
            self.width,
            self.height,
            self.framerate,
            self.profile,
            self.bitrate,
            "bgr24",
        )
        self.mem = SharedFrame(
            self.name, (self.height, self.width, 3), mode="w", dtype=self.dtype
        )


class VLCCamera(Sensor):
    def __init__(self, idx) -> None:
        super().__init__()
        self.idx = idx

    def config(self):
        from . import hl2ss

        options = [
            hl2ss.StreamPort.RM_VLC_LEFTFRONT,
            hl2ss.StreamPort.RM_VLC_LEFTLEFT,
            hl2ss.StreamPort.RM_VLC_RIGHTFRONT,
            hl2ss.StreamPort.RM_VLC_RIGHTRIGHT,
        ]
        self.name = f"grayscale-{self.idx}"
        port = options[self.idx]
        self.mode = hl2ss.StreamMode.MODE_0
        self.profile = hl2ss.VideoProfile.H265_MAIN
        self.bitrate = 1 * 1024 * 1024
        self.dtype = "uint8"
        self.client = hl2ss.rx_rm_vlc(
            self.host,
            port,
            hl2ss.ChunkSize.RM_VLC,
            self.mode,
            self.profile,
            self.bitrate,
            "bgr24",
        )
        self.mem = SharedFrame(self.name, (480, 640, 3), mode="w", dtype=self.dtype)


class DepthCamera(Sensor):
    def config(self):
        from . import hl2ss

        self.port = hl2ss.StreamPort.RM_DEPTH_LONGTHROW
        self.mode = hl2ss.StreamMode.MODE_0
        self.client = hl2ss.rx_rm_depth(
            self.host, self.port, hl2ss.ChunkSize.RM_DEPTH_LONGTHROW, self.mode
        )
        self.mem = {}
        self.mem["depth"] = SharedFrame("depth", (288, 320, 1), mode="w", dtype="int16")
        self.mem["ab"] = SharedFrame("ab", (288, 320, 1), mode="w", dtype="int16")

    def run(self):
        self.config()
        self.client.open()
        while self.running:
            data = self.client.get_next_packet()
            self.mem["depth"].write(data.payload.depth, data.timestamp)
            self.mem["ab"].write(data.payload.ab, data.timestamp)


class IMU(Sensor):
    def __init__(self, name) -> None:
        super().__init__()
        assert name in ["acc", "gyro", "mag"], print("Mode not found")
        self.name = name

    def config(self):
        from . import hl2ss

        options = {
            "acc": [
                hl2ss.StreamPort.RM_IMU_ACCELEROMETER,
                hl2ss.ChunkSize.RM_IMU_ACCELEROMETER,
                (28, 93),
            ],
            "gyro": [
                hl2ss.StreamPort.RM_IMU_GYROSCOPE,
                hl2ss.ChunkSize.RM_IMU_GYROSCOPE,
                (28, 315),
            ],
            "mag": [
                hl2ss.StreamPort.RM_IMU_MAGNETOMETER,
                hl2ss.ChunkSize.RM_IMU_MAGNETOMETER,
                (28, 11),
            ],
        }

        self.mode = hl2ss.StreamMode.MODE_0
        self.dtype = "uint8"
        self.port, self.chunk, self.shape = options[self.name]
        self.client = hl2ss.rx_rm_imu(self.host, self.port, self.chunk, self.mode)
        self.mem = SharedFrame(self.name, self.shape, mode="w", dtype=self.dtype)


class Microphone(Sensor):
    def config(self):
        from . import hl2ss

        self.name = "audio"
        self.shape = [2, 1024]
        # self.shape = [1, 2048]
        # self.dtype = "float32"
        self.dtype = "int16"
        self.client = hl2ss.rx_mc(
            self.host,
            hl2ss.StreamPort.MICROPHONE,
            hl2ss.ChunkSize.MICROPHONE,
            hl2ss.AudioProfile.AAC_24000,
        )
        self.mem = SharedFrame(self.name, self.shape, mode="w", dtype=self.dtype)


class Mevo(Sensor):
    def ndi_init(self):
        if system == "Darwin":
            return 0
        import NDIlib as ndi

        if not ndi.initialize():
            return 0
        ndi_find = ndi.find_create_v2()
        if ndi_find is None:
            return 0
        for i in range(10):
            ndi.find_wait_for_sources(ndi_find, 1000)
            sources = ndi.find_get_current_sources(ndi_find)
            try:
                # target_source = [sources[i] for i, s in enumerate(sources) if self.camera_id in s.ndi_name][0]
                target_source = [
                    sources[i]
                    for i, s in enumerate(sources)
                    if "MEVO" in s.ndi_name and "PJ5" in s.ndi_name
                ][0]
                break
            except:
                print(f"Found {len(sources)} cameras but not match")
        ndi_recv_create = ndi.RecvCreateV3()
        ndi_recv_create.color_format = ndi.RECV_COLOR_FORMAT_BGRX_BGRA
        ndi_recv = ndi.recv_create_v3(ndi_recv_create)
        ndi.recv_connect(ndi_recv, target_source)
        ndi.find_destroy(ndi_find)
        self.ndi_recv = ndi_recv

    def config(self):
        # self.width = 1280
        self.width = 1920
        self.height = 1080
        # self.height = 720
        self.dtype = "uint8"

        self.name = "mevo"
        self.mem = {}
        self.mem["video"] = SharedFrame(
            "mevo-video", (self.height, self.width, 3), mode="w", dtype=self.dtype
        )
        self.mem["audio"] = SharedFrame(
            "mevo-audio", (2, 1024), mode="w", dtype="float32"
        )

    def run(self):
        if system == "Darwin":
            return
        import NDIlib as ndi

        self.ndi_init()
        self.config()
        start = time.time()
        count = 0
        while self.running:
            count += 1
            if count % 100 == 0:
                print(f"running {time.time()-start:.2f} second")

            t, v, a, _ = ndi.recv_capture_v2(self.ndi_recv, 1000)
            if t == ndi.FRAME_TYPE_VIDEO:
                frame = np.copy(v.data[:, :, :-1])
                ndi.recv_free_video_v2(self.ndi_recv, v)
                self.mem["video"].write(frame, 0)
                continue
            if t == ndi.FRAME_TYPE_AUDIO:
                # audio = np.copy(a.data)
                # data = np.zeros((a.no_channels, a.no_samples), np.int16)
                # interleaved = ndi.AudioFrameInterleaved16s()
                # interleaved.data = data
                # ndi.util_audio_to_interleaved_16s_v2(a, interleaved)
                # # timeout = interleaved.no_samples * 20 / interleaved.sample_rate
                # audio = data
                # ndi.recv_free_audio_v2(self.ndi_recv, a)
                self.mem["audio"].write(a.data, 0)

                # print('Audio data received (%d samples).' % a.no_samples)
            if t == ndi.FRAME_TYPE_VIDEO:
                frame = np.copy(v.data[:, :, :-1])
                ndi.recv_free_video_v2(self.ndi_recv, v)
                self.mem["video"].write(frame, 0)
                continue
            if t == ndi.FRAME_TYPE_AUDIO:
                # audio = np.copy(a.data)
                # data = np.zeros((a.no_channels, a.no_samples), np.int16)
                # interleaved = ndi.AudioFrameInterleaved16s()
                # interleaved.data = data
                # ndi.util_audio_to_interleaved_16s_v2(a, interleaved)
                # # timeout = interleaved.no_samples * 20 / interleaved.sample_rate
                # audio = data
                # ndi.recv_free_audio_v2(self.ndi_recv, a)
                self.mem["audio"].write(a.data, 0)

                # print('Audio data received (%d samples).' % a.no_samples)
