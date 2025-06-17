
from fractions import Fraction

import io
import numpy as np
import socket
import struct
import time
import cv2
import av
import os
import tarfile


# Stream TCP Ports
class StreamPort:
    RM_VLC_LEFTFRONT     = 3800
    RM_VLC_LEFTLEFT      = 3801
    RM_VLC_RIGHTFRONT    = 3802
    RM_VLC_RIGHTRIGHT    = 3803
    RM_DEPTH_AHAT        = 3804
    RM_DEPTH_LONGTHROW   = 3805
    RM_IMU_ACCELEROMETER = 3806
    RM_IMU_GYROSCOPE     = 3807
    RM_IMU_MAGNETOMETER  = 3808
    REMOTE_CONFIGURATION = 3809
    PERSONAL_VIDEO       = 3810
    MICROPHONE           = 3811
    SPATIAL_INPUT        = 3812


# Default Chunk Sizes
class ChunkSize:
    RM_VLC               = 1024
    RM_DEPTH_AHAT        = 4096
    RM_DEPTH_LONGTHROW   = 4096
    RM_IMU_ACCELEROMETER = 2048
    RM_IMU_GYROSCOPE     = 4096
    RM_IMU_MAGNETOMETER  = 256
    PERSONAL_VIDEO       = 4096
    MICROPHONE           = 512
    SPATIAL_INPUT        = 1024
    SINGLE_TRANSFER      = 4096


# Stream Operating Mode
# 0: device data (e.g. video)
# 1: device data + location data (e.g. video + camera pose)
# 2: device constants (e.g. camera intrinsics)
class StreamMode:
    MODE_0 = 0
    MODE_1 = 1
    MODE_2 = 2


# Video Encoder Configuration
# 0: H264 base
# 1: H264 main
# 2: H264 high
# 3: H265 main (HEVC)
class VideoProfile:
    H264_BASE = 0
    H264_MAIN = 1
    H264_HIGH = 2
    H265_MAIN = 3


# Audio Encoder Configuration
# 0: AAC 12000 bytes/s
# 1: AAC 16000 bytes/s
# 2: AAC 20000 bytes/s
# 3: AAC 24000 bytes/s
class AudioProfile:
    AAC_12000 = 0
    AAC_16000 = 1
    AAC_20000 = 2
    AAC_24000 = 3


# RM VLC Parameters
class Parameters_RM_VLC:
    WIDTH  = 640
    HEIGHT = 480
    FPS    = 30
    PIXELS = WIDTH * HEIGHT
    SHAPE  = (HEIGHT, WIDTH)
    FORMAT = 'yuv420p'
    PERIOD = 1 / FPS


# RM Depth Long Throw Parameters
class Parameters_RM_DEPTH_LONGTHROW:
    WIDTH  = 320
    HEIGHT = 288
    FPS    = 5
    PIXELS = WIDTH * HEIGHT
    SHAPE  = (HEIGHT, WIDTH)
    PERIOD = 1 / FPS


# PV Parameters
class Parameters_PV:
    FORMAT = 'yuv420p'


# MC Parameters
class Parameters_MC:
    SAMPLE_RATE = 48000
    GROUP_SIZE  = 1024
    CHANNELS    = 2
    FORMAT      = 'fltp'
    LAYOUT      = 'stereo'
    CONTAINER   = 'adts'
    PERIOD      = GROUP_SIZE / SAMPLE_RATE


# SI Parameters
class Parameters_SI:
    SAMPLE_RATE = 60
    PERIOD      = 1 / SAMPLE_RATE


# Time base for all timestamps
class TimeBase:
    HUNDREDS_OF_NANOSECONDS = 10*1000*1000


# Hand joints
class HandJointKind:
    Palm = 0
    Wrist = 1
    ThumbMetacarpal = 2
    ThumbProximal = 3
    ThumbDistal = 4
    ThumbTip = 5
    IndexMetacarpal = 6
    IndexProximal = 7
    IndexIntermediate = 8
    IndexDistal = 9
    IndexTip = 10
    MiddleMetacarpal = 11
    MiddleProximal = 12
    MiddleIntermediate = 13
    MiddleDistal = 14
    MiddleTip = 15
    RingMetacarpal = 16
    RingProximal = 17
    RingIntermediate = 18
    RingDistal = 19
    RingTip = 20
    LittleMetacarpal = 21
    LittleProximal = 22
    LittleIntermediate = 23
    LittleDistal = 24
    LittleTip = 25
    TOTAL = 26


class _SIZEOF:
    BYTE = 1
    LONGLONG = 8
    INT = 4
    FLOAT = 4


#------------------------------------------------------------------------------
# Network Client
#------------------------------------------------------------------------------

class client:
    def open(self, host, port):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))

    def sendall(self, data):
        self._socket.sendall(data)

    def recv(self, chunk_size):
        chunk = self._socket.recv(chunk_size)
        if (len(chunk) <= 0):
            raise Exception('connection closed')
        return chunk

    def download(self, total, chunk_size):
        data = bytearray()

        if (chunk_size > total):
            chunk_size = total

        while (total > 0):
            chunk = self.recv(chunk_size)
            data.extend(chunk)
            total -= len(chunk)
            if (chunk_size > total):
                chunk_size = total

        if (total != 0):
            raise Exception('download failed')

        return data

    def close(self):
        self._socket.close()


#------------------------------------------------------------------------------
# Packet Unpacker
#------------------------------------------------------------------------------

class packet:
    def __init__(self, timestamp, payload, pose):
        self.timestamp = timestamp
        self.payload   = payload
        self.pose      = pose

    def pack(self, mode):
        buffer = bytearray()
        buffer.extend(struct.pack('<QI', self.timestamp, len(self.payload)))
        buffer.extend(self.payload)
        if (mode == StreamMode.MODE_1):
            buffer.extend(self.pose.tobytes())
        return buffer


class unpacker:
    def __init__(self, mode):
        self._mode = mode
        self._state = 0
        self._buffer = bytearray()
        self._timestamp = None
        self._size = None
        self._payload = None
        self._pose = None

    def extend(self, chunk):
        self._buffer.extend(chunk)

    def unpack(self):        
        length = len(self._buffer)
        
        while True:
            if (self._state == 0):
                if (length >= 12):
                    header = struct.unpack('<QI', self._buffer[:12])
                    self._timestamp = header[0]
                    self._size = 12 + header[1]
                    if (self._mode == StreamMode.MODE_1):
                        self._size += 64
                    self._state = 1
                    continue
            elif (self._state == 1):
                if (length >= self._size):
                    if (self._mode == StreamMode.MODE_1):
                        payload_end = self._size - 64
                        self._pose = np.frombuffer(self._buffer[payload_end:self._size], dtype=np.float32).reshape((4, 4))
                    else:
                        payload_end = self._size
                    self._payload = self._buffer[12:payload_end]
                    self._buffer = self._buffer[self._size:]
                    self._state = 0
                    return True
            return False

    def get(self):
        return packet(self._timestamp, self._payload, self._pose)


#------------------------------------------------------------------------------
# Packet Gatherer
#------------------------------------------------------------------------------

class gatherer:
    def open(self, host, port, chunk_size, mode):
        self._client = client()
        self._unpacker = unpacker(mode)
        self._chunk_size = chunk_size

        self._client.open(host, port)
        
    def sendall(self, data):
        self._client.sendall(data)

    def get_next_packet(self):
        while True:
            self._unpacker.extend(self._client.recv(self._chunk_size))
            if (self._unpacker.unpack()):
                return self._unpacker.get()

    def close(self):
        self._client.close()


class packet_stream:
    def __init__(self, client):
        self._client = client
    
    def get_next_packet(self):
        return self._client.get_next_packet()

    def close(self):
        return self._client.close()


#------------------------------------------------------------------------------
# File I/O
#------------------------------------------------------------------------------

class raw_writer:
    def open(self, filename, mode):
        self._data = open(filename, 'wb')
        self._data.write(struct.pack('<B', mode))
        self._mode = mode

    def write(self, data):
        self._data.write(data.pack(self._mode))

    def close(self):
        self._data.close()


class raw_reader:
    def open(self, filename, chunk_size):
        self._data = open(filename, 'rb')
        self._mode = struct.unpack('<B', self._data.read(_SIZEOF.BYTE))[0]
        self._unpacker = unpacker(self._mode)
        self._chunk_size = chunk_size
        self._eof = False
        
    def read(self):
        while (True):
            if (self._unpacker.unpack()):
                return self._unpacker.get()
            elif (self._eof):
                return None
            else:
                chunk = self._data.read(self._chunk_size)
                self._eof = len(chunk) < self._chunk_size
                self._unpacker.extend(chunk)

    def close(self):
        self._data.close()


#------------------------------------------------------------------------------
# RM Depth Unpacker
#------------------------------------------------------------------------------

class RM_Depth_Frame:
    def __init__(self, depth, ab):
        self.depth = depth
        self.ab    = ab


def unpack_rm_depth(payload):
    composite = cv2.imdecode(np.frombuffer(payload, dtype=np.uint8), cv2.IMREAD_UNCHANGED)
    h, w, _ = composite.shape
    interleaved = composite.view(np.uint16).reshape((h, w, 2))
    depth, ab = np.dsplit(interleaved, 2)
    return RM_Depth_Frame(depth, ab)


#------------------------------------------------------------------------------
# RM IMU Unpacker
#------------------------------------------------------------------------------

class RM_IMU_Sample:
    def __init__(self, sensor_ticks_ns, x, y, z):
        self.sensor_ticks_ns = sensor_ticks_ns
        self.x               = x
        self.y               = y
        self.z               = z


class unpack_rm_imu:
    def __init__(self, payload):
        self._count = int(len(payload) / 28)
        self._batch = payload

    def get_count(self):
        return self._count

    def get_sample(self, index):
        data = struct.unpack('<QQfff', self._batch[(index * 28):((index + 1) * 28)])
        return RM_IMU_Sample(data[0], data[2], data[3], data[4])


#------------------------------------------------------------------------------
# SI Unpacker
#------------------------------------------------------------------------------

class _SI_Field:
    HEAD  = 1
    EYE   = 2
    LEFT  = 4
    RIGHT = 8


class SI_HeadPose:
    def __init__(self, position, forward, up):
        self.position = position
        self.forward  = forward
        self.up       = up


class SI_EyeRay:
    def __init__(self, origin, direction):
        self.origin    = origin
        self.direction = direction


class SI_HandJointPose:
    def __init__(self, orientation, position, radius, accuracy):
        self.orientation = orientation
        self.position    = position
        self.radius      = radius
        self.accuracy    = accuracy


class _Mode0Layout_SI_Hand:
    BEGIN_ORIENTATION = 0
    END_ORIENTATION   = BEGIN_ORIENTATION + 4*_SIZEOF.FLOAT
    BEGIN_POSITION    = END_ORIENTATION
    END_POSITION      = BEGIN_POSITION + 3*_SIZEOF.FLOAT
    BEGIN_RADIUS      = END_POSITION
    END_RADIUS        = BEGIN_RADIUS + 1*_SIZEOF.FLOAT
    BEGIN_ACCURACY    = END_RADIUS
    END_ACCURACY      = BEGIN_ACCURACY + 1*_SIZEOF.INT
    BYTE_COUNT        = END_ACCURACY


class _Mode0Layout_SI:
    BEGIN_VALID         = 0
    END_VALID           = BEGIN_VALID + 1
    BEGIN_HEAD_POSITION = END_VALID
    END_HEAD_POSITION   = BEGIN_HEAD_POSITION + 3*_SIZEOF.FLOAT
    BEGIN_HEAD_FORWARD  = END_HEAD_POSITION
    END_HEAD_FORWARD    = BEGIN_HEAD_FORWARD + 3*_SIZEOF.FLOAT
    BEGIN_HEAD_UP       = END_HEAD_FORWARD
    END_HEAD_UP         = BEGIN_HEAD_UP + 3*_SIZEOF.FLOAT
    BEGIN_EYE_ORIGIN    = END_HEAD_UP
    END_EYE_ORIGIN      = BEGIN_EYE_ORIGIN + 3*_SIZEOF.FLOAT
    BEGIN_EYE_DIRECTION = END_EYE_ORIGIN
    END_EYE_DIRECTION   = BEGIN_EYE_DIRECTION + 3*_SIZEOF.FLOAT
    BEGIN_HAND_LEFT     = END_EYE_DIRECTION
    END_HAND_LEFT       = BEGIN_HAND_LEFT + HandJointKind.TOTAL * _Mode0Layout_SI_Hand.BYTE_COUNT
    BEGIN_HAND_RIGHT    = END_HAND_LEFT
    END_HAND_RIGHT      = BEGIN_HAND_RIGHT + HandJointKind.TOTAL * _Mode0Layout_SI_Hand.BYTE_COUNT


class unpack_si_hand:
    def __init__(self, payload):
        self._data = payload

    def get_joint_pose(self, joint):
        begin = joint * _Mode0Layout_SI_Hand.BYTE_COUNT
        end = begin + _Mode0Layout_SI_Hand.BYTE_COUNT
        data = self._data[begin:end]

        orientation = np.frombuffer(data[_Mode0Layout_SI_Hand.BEGIN_ORIENTATION : _Mode0Layout_SI_Hand.END_ORIENTATION], dtype=np.float32)
        position    = np.frombuffer(data[_Mode0Layout_SI_Hand.BEGIN_POSITION    : _Mode0Layout_SI_Hand.END_POSITION],    dtype=np.float32)
        radius      = np.frombuffer(data[_Mode0Layout_SI_Hand.BEGIN_RADIUS      : _Mode0Layout_SI_Hand.END_RADIUS],      dtype=np.float32)
        accuracy    = np.frombuffer(data[_Mode0Layout_SI_Hand.BEGIN_ACCURACY    : _Mode0Layout_SI_Hand.END_ACCURACY],    dtype=np.int32)

        return SI_HandJointPose(orientation, position, radius, accuracy)


class unpack_si:
    def __init__(self, payload):
        self._data = payload
        self._valid = np.frombuffer(payload[_Mode0Layout_SI.BEGIN_VALID : _Mode0Layout_SI.END_VALID], dtype=np.uint8)

    def is_valid_head_pose(self):
        return (self._valid & _SI_Field.HEAD) != 0

    def is_valid_eye_ray(self):
        return (self._valid & _SI_Field.EYE) != 0

    def is_valid_hand_left(self):
        return (self._valid & _SI_Field.LEFT) != 0

    def is_valid_hand_right(self):
        return (self._valid & _SI_Field.RIGHT) != 0

    def get_head_pose(self):
        position = np.frombuffer(self._data[_Mode0Layout_SI.BEGIN_HEAD_POSITION : _Mode0Layout_SI.END_HEAD_POSITION], dtype=np.float32)
        forward  = np.frombuffer(self._data[_Mode0Layout_SI.BEGIN_HEAD_FORWARD  : _Mode0Layout_SI.END_HEAD_FORWARD],  dtype=np.float32)
        up       = np.frombuffer(self._data[_Mode0Layout_SI.BEGIN_HEAD_UP       : _Mode0Layout_SI.END_HEAD_UP],       dtype=np.float32)

        return SI_HeadPose(position, forward, up)

    def get_eye_ray(self):
        origin    = np.frombuffer(self._data[_Mode0Layout_SI.BEGIN_EYE_ORIGIN    : _Mode0Layout_SI.END_EYE_ORIGIN],    dtype=np.float32)
        direction = np.frombuffer(self._data[_Mode0Layout_SI.BEGIN_EYE_DIRECTION : _Mode0Layout_SI.END_EYE_DIRECTION], dtype=np.float32)

        return SI_EyeRay(origin, direction)

    def get_hand_left(self):
        return unpack_si_hand(self._data[_Mode0Layout_SI.BEGIN_HAND_LEFT : _Mode0Layout_SI.END_HAND_LEFT])

    def get_hand_right(self):
        return unpack_si_hand(self._data[_Mode0Layout_SI.BEGIN_HAND_RIGHT : _Mode0Layout_SI.END_HAND_RIGHT])


#------------------------------------------------------------------------------
# Codecs
#------------------------------------------------------------------------------

def get_video_codec_name(profile):
    if   (profile == VideoProfile.H264_BASE):
        return 'h264'
    elif (profile == VideoProfile.H264_MAIN):
        return 'h264'
    elif (profile == VideoProfile.H264_HIGH):
        return 'h264'
    elif (profile == VideoProfile.H265_MAIN):
        return 'hevc'
    else:
        return None


def get_audio_codec_name(profile):
    if   (profile == AudioProfile.AAC_12000):
        return 'aac'
    elif (profile == AudioProfile.AAC_16000):
        return 'aac'
    elif (profile == AudioProfile.AAC_20000):
        return 'aac'
    elif (profile == AudioProfile.AAC_24000):
        return 'aac'
    else:
        return None


def get_audio_codec_bitrate(profile):
    if   (profile == AudioProfile.AAC_12000):
        return 12000*8
    elif (profile == AudioProfile.AAC_16000):
        return 16000*8
    elif (profile == AudioProfile.AAC_20000):
        return 20000*8
    elif (profile == AudioProfile.AAC_24000):
        return 24000*8
    else:
        return None


#------------------------------------------------------------------------------
# Stream Configuration
#------------------------------------------------------------------------------

def _create_configuration_for_mode(mode):
    return struct.pack('<B', mode)


def _create_configuration_for_video(mode, width, height, framerate, profile, bitrate):
    return struct.pack('<BHHBBI', mode, width, height, framerate, profile, bitrate)


def _create_configuration_for_audio(profile):
    return struct.pack('<B', profile)


#------------------------------------------------------------------------------
# Mode 0 and Mode 1 Data Acquisition
#------------------------------------------------------------------------------

def connect_client_rm_vlc(host, port, chunk_size, mode, profile, bitrate):
    c = gatherer()
    c.open(host, port, chunk_size, mode)
    c.sendall(_create_configuration_for_video(mode, Parameters_RM_VLC.WIDTH, Parameters_RM_VLC.HEIGHT, Parameters_RM_VLC.FPS, profile, bitrate))
    return packet_stream(c)


def connect_client_rm_depth(host, port, chunk_size, mode):
    c = gatherer()
    c.open(host, port, chunk_size, mode)
    c.sendall(_create_configuration_for_mode(mode))
    return packet_stream(c)


def connect_client_rm_imu(host, port, chunk_size, mode):
    c = gatherer()
    c.open(host, port, chunk_size, mode)
    c.sendall(_create_configuration_for_mode(mode))
    return packet_stream(c)


def connect_client_pv(host, port, chunk_size, mode, width, height, framerate, profile, bitrate):
    c = gatherer()
    c.open(host, port, chunk_size, mode)
    c.sendall(_create_configuration_for_video(mode, width, height, framerate, profile, bitrate))
    return packet_stream(c)


def connect_client_mc(host, port, chunk_size, profile):
    c = gatherer()
    c.open(host, port, chunk_size, StreamMode.MODE_0)
    c.sendall(_create_configuration_for_audio(profile))
    return packet_stream(c)


def connect_client_si(host, port, chunk_size):
    c = gatherer()
    c.open(host, port, chunk_size, StreamMode.MODE_0)
    return packet_stream(c)


#------------------------------------------------------------------------------
# Mode 2 Data Acquisition
#------------------------------------------------------------------------------

class _Mode2Layout_RM_VLC:
    BEGIN_UV2X       = 0
    END_UV2X         = BEGIN_UV2X + Parameters_RM_VLC.PIXELS
    BEGIN_UV2Y       = END_UV2X
    END_UV2Y         = BEGIN_UV2Y + Parameters_RM_VLC.PIXELS
    BEGIN_EXTRINSICS = END_UV2Y
    END_EXTRINSICS   = BEGIN_EXTRINSICS + 16
    FLOAT_COUNT      = 2*Parameters_RM_VLC.PIXELS + 16


class _Mode2Layout_RM_DEPTH_LONGTHROW:
    BEGIN_UV2X       = 0
    END_UV2X         = BEGIN_UV2X + Parameters_RM_DEPTH_LONGTHROW.PIXELS
    BEGIN_UV2Y       = END_UV2X
    END_UV2Y         = BEGIN_UV2Y + Parameters_RM_DEPTH_LONGTHROW.PIXELS
    BEGIN_EXTRINSICS = END_UV2Y
    END_EXTRINSICS   = BEGIN_EXTRINSICS + 16
    BEGIN_SCALE      = END_EXTRINSICS
    END_SCALE        = BEGIN_SCALE + 1
    FLOAT_COUNT      = 2*Parameters_RM_DEPTH_LONGTHROW.PIXELS + 16 + 1


class _Mode2Layout_RM_IMU:
    BEGIN_EXTRINSICS = 0
    END_EXTRINSICS   = BEGIN_EXTRINSICS + 16
    FLOAT_COUNT      = 16


class _Mode2Layout_PV:
    BEGIN_FOCALLENGTH          = 0
    END_FOCALLENGTH            = BEGIN_FOCALLENGTH + 2
    BEGIN_PRINCIPALPOINT       = END_FOCALLENGTH
    END_PRINCIPAL_POINT        = BEGIN_PRINCIPALPOINT + 2
    BEGIN_RADIALDISTORTION     = END_PRINCIPAL_POINT
    END_RADIALDISTORTION       = BEGIN_RADIALDISTORTION + 3
    BEGIN_TANGENTIALDISTORTION = END_RADIALDISTORTION
    END_TANGENTIALDISTORTION   = BEGIN_TANGENTIALDISTORTION + 2
    BEGIN_PROJECTION           = END_TANGENTIALDISTORTION
    END_PROJECTION             = BEGIN_PROJECTION + 16
    FLOAT_COUNT                = 2 + 2 + 3 + 2 + 16


class Mode2_RM_VLC:
    def __init__(self, uv2xy, extrinsics):
        self.uv2xy      = uv2xy
        self.extrinsics = extrinsics


class Mode2_RM_DEPTH:
    def __init__(self, uv2xy, extrinsics, scale):
        self.uv2xy      = uv2xy
        self.extrinsics = extrinsics
        self.scale      = scale


class Mode2_RM_IMU:
    def __init__(self, extrinsics):
        self.extrinsics = extrinsics


class Mode2_PV:
    def __init__(self, focal_length, principal_point, radial_distortion, tangential_distortion, projection):
        self.focal_length          = focal_length
        self.principal_point       = principal_point
        self.radial_distortion     = radial_distortion
        self.tangential_distortion = tangential_distortion
        self.projection            = projection


def _download_mode2_data(host, port, configuration, bytes):
    c = client()

    c.open(host, port)
    c.sendall(configuration)
    data = c.download(bytes, ChunkSize.SINGLE_TRANSFER)
    c.close()

    return data


def download_calibration_rm_vlc(host, port):
    data   = _download_mode2_data(host, port, _create_configuration_for_mode(StreamMode.MODE_2), _Mode2Layout_RM_VLC.FLOAT_COUNT * _SIZEOF.FLOAT)
    floats = np.frombuffer(data, dtype=np.float32)

    uv2x       = floats[_Mode2Layout_RM_VLC.BEGIN_UV2X       : _Mode2Layout_RM_VLC.END_UV2X].reshape(Parameters_RM_VLC.SHAPE)
    uv2y       = floats[_Mode2Layout_RM_VLC.BEGIN_UV2Y       : _Mode2Layout_RM_VLC.END_UV2Y].reshape(Parameters_RM_VLC.SHAPE)
    extrinsics = floats[_Mode2Layout_RM_VLC.BEGIN_EXTRINSICS : _Mode2Layout_RM_VLC.END_EXTRINSICS].reshape((4, 4))

    return Mode2_RM_VLC(np.dstack((uv2x, uv2y)), extrinsics)


def download_calibration_rm_depth(host, port):
    data   = _download_mode2_data(host, port, _create_configuration_for_mode(StreamMode.MODE_2), _Mode2Layout_RM_DEPTH_LONGTHROW.FLOAT_COUNT * _SIZEOF.FLOAT)
    floats = np.frombuffer(data, dtype=np.float32)

    uv2x       = floats[_Mode2Layout_RM_DEPTH_LONGTHROW.BEGIN_UV2X       : _Mode2Layout_RM_DEPTH_LONGTHROW.END_UV2X].reshape(Parameters_RM_DEPTH_LONGTHROW.SHAPE)
    uv2y       = floats[_Mode2Layout_RM_DEPTH_LONGTHROW.BEGIN_UV2Y       : _Mode2Layout_RM_DEPTH_LONGTHROW.END_UV2Y].reshape(Parameters_RM_DEPTH_LONGTHROW.SHAPE)
    extrinsics = floats[_Mode2Layout_RM_DEPTH_LONGTHROW.BEGIN_EXTRINSICS : _Mode2Layout_RM_DEPTH_LONGTHROW.END_EXTRINSICS].reshape((4, 4))
    scale      = floats[_Mode2Layout_RM_DEPTH_LONGTHROW.BEGIN_SCALE      : _Mode2Layout_RM_DEPTH_LONGTHROW.END_SCALE]

    return Mode2_RM_DEPTH(np.dstack((uv2x, uv2y)), extrinsics, scale)


def download_calibration_rm_imu(host, port):
    data   = _download_mode2_data(host, port, _create_configuration_for_mode(StreamMode.MODE_2), _Mode2Layout_RM_IMU.FLOAT_COUNT * _SIZEOF.FLOAT)
    floats = np.frombuffer(data, dtype=np.float32)

    extrinsics = floats[_Mode2Layout_RM_IMU.BEGIN_EXTRINSICS : _Mode2Layout_RM_IMU.END_EXTRINSICS].reshape((4, 4))

    return Mode2_RM_IMU(extrinsics)


def download_calibration_pv(host, port, width, height, framerate, profile, bitrate):
    data   = _download_mode2_data(host, port, _create_configuration_for_video(StreamMode.MODE_2, width, height, framerate, profile, bitrate), _Mode2Layout_PV.FLOAT_COUNT * _SIZEOF.FLOAT)
    floats = np.frombuffer(data, dtype=np.float32)

    focal_length          = floats[_Mode2Layout_PV.BEGIN_FOCALLENGTH          : _Mode2Layout_PV.END_FOCALLENGTH]
    principal_point       = floats[_Mode2Layout_PV.BEGIN_PRINCIPALPOINT       : _Mode2Layout_PV.END_PRINCIPAL_POINT]
    radial_distortion     = floats[_Mode2Layout_PV.BEGIN_RADIALDISTORTION     : _Mode2Layout_PV.END_RADIALDISTORTION]
    tangential_distortion = floats[_Mode2Layout_PV.BEGIN_TANGENTIALDISTORTION : _Mode2Layout_PV.END_TANGENTIALDISTORTION]
    projection            = floats[_Mode2Layout_PV.BEGIN_PROJECTION           : _Mode2Layout_PV.END_PROJECTION].reshape((4, 4))

    projection[0,0] = -projection[0,0]
    projection[1,1] = -projection[1,1]
    projection[2,0] = width  - projection[3,0]
    projection[2,1] = height - projection[3,1]
    projection[3,0] = 0
    projection[3,1] = 0

    return Mode2_PV(focal_length, principal_point, radial_distortion, tangential_distortion, projection)


#------------------------------------------------------------------------------
# Receiver Wrappers
#------------------------------------------------------------------------------

class rx_rm_vlc:
    def __init__(self, host, port, chunk, mode, profile, bitrate, format):
        self.host = host
        self.port = port
        self.chunk = chunk
        self.mode = mode
        self.profile = profile
        self.bitrate = bitrate
        self.format = format

    def open(self):
        self._codec = av.CodecContext.create(get_video_codec_name(self.profile), 'r')
        self._client = connect_client_rm_vlc(self.host, self.port, self.chunk, self.mode, self.profile, self.bitrate)
        self.get_next_packet()

    def get_next_packet(self):
        data = self._client.get_next_packet()
        for packet in self._codec.parse(data.payload):
            for frame in self._codec.decode(packet):
                data.payload = frame.to_ndarray(format=self.format)
        return data

    def close(self):
        self._client.close()


class rx_rm_depth:
    def __init__(self, host, port, chunk, mode):
        self.host = host
        self.port = port
        self.chunk = chunk
        self.mode = mode

    def open(self):
        self._client = connect_client_rm_depth(self.host, self.port, self.chunk, self.mode)

    def get_next_packet(self):
        data = self._client.get_next_packet()
        data.payload = unpack_rm_depth(data.payload)
        return data

    def close(self):
        self._client.close()


class rx_rm_imu:
    def __init__(self, host, port, chunk, mode):
        self.host = host
        self.port = port
        self.chunk = chunk
        self.mode = mode

    def open(self):
        self._client = connect_client_rm_imu(self.host, self.port, self.chunk, self.mode)

    def get_next_packet(self):
        return self._client.get_next_packet()

    def close(self):
        self._client.close()


class rx_pv:
    def __init__(self, host, port, chunk, mode, width, height, framerate, profile, bitrate, format):
        self.host = host
        self.port = port
        self.chunk = chunk
        self.mode = mode
        self.width = width
        self.height = height
        self.framerate = framerate
        self.profile = profile
        self.bitrate = bitrate
        self.format = format

    def open(self):
        self._codec = av.CodecContext.create(get_video_codec_name(self.profile), 'r')
        self._client = connect_client_pv(self.host, self.port, self.chunk, self.mode, self.width, self.height, self.framerate, self.profile, self.bitrate)
        self.get_next_packet()

    def get_next_packet(self):
        data = self._client.get_next_packet()
        for packet in self._codec.parse(data.payload):
            for frame in self._codec.decode(packet):
                data.payload = frame.to_ndarray(format=self.format)
        return data

    def close(self):
        self._client.close()


class rx_mc:
    def __init__(self, host, port, chunk, profile):
        self.host = host
        self.port = port
        self.chunk = chunk
        self.profile = profile
        # ======================
        self.resampler = av.audio.resampler.AudioResampler(format='s16', layout='stereo', rate=48000)

    def open(self):
        self._codec = av.CodecContext.create(get_audio_codec_name(self.profile), 'r')
        self._client = connect_client_mc(self.host, self.port, self.chunk, self.profile)

    def get_next_packet(self):
        data = self._client.get_next_packet()
        for packet in self._codec.parse(data.payload):
            for frame in self._codec.decode(packet):
                # data.payload = frame.to_ndarray()
                # ================================================
                for audio in self.resampler.resample(frame):
                    audio_arr = audio.to_ndarray()
                    data.payload=audio_arr
                    # print(audio_arr.shape)
                    data.payload = np.concatenate((audio_arr[0:1,0::2],audio_arr[0:1,1::2]))
                    # print()
                # =================================================
        return data

    def close(self):
        self._client.close()


class rx_si:
    def __init__(self, host, port, chunk):
        self.host = host
        self.port = port
        self.chunk = chunk

    def open(self):
        self._client = connect_client_si(self.host, self.port, self.chunk)

    def get_next_packet(self):
        return self._client.get_next_packet()

    def close(self):
        self._client.close()


#------------------------------------------------------------------------------
# Writers
#------------------------------------------------------------------------------

class _wr_ancillary:
    def __init__(self, filename, mode):
        self._file = open(filename, 'wb')
        self._file.write(struct.pack('<B', mode))
        self._mode = mode
        
    def write(self, data):
        self._file.write(struct.pack('<Q', data.timestamp))
        if (self._mode == StreamMode.MODE_1):
            self._file.write(data.pose.tobytes())

    def close(self):
        self._file.close()


class wr_rm_vlc:
    def __init__(self, path, name, mode, codec, bitrate, format):
        self.path = path
        self.name = name
        self.codec = codec
        self.bitrate = bitrate
        self.format = format
        self.mode = mode

    def _encode(self, frame):
        for packet in self._stream.encode(frame):
            self._frames.mux(packet)

    def open(self):
        self._ancillary = _wr_ancillary(os.path.join(self.path, 'rm_vlc_' + self.name + '_ancillary.bin'), self.mode)
        self._frames = av.open(os.path.join(self.path, 'rm_vlc_' + self.name + '.mp4'), mode='w')
        self._stream = self._frames.add_stream(self.codec, rate=Parameters_RM_VLC.FPS)
        self._stream.width = Parameters_RM_VLC.WIDTH
        self._stream.height = Parameters_RM_VLC.HEIGHT
        self._stream.pix_fmt = Parameters_RM_VLC.FORMAT
        self._stream.bit_rate = self.bitrate

    def write(self, data):
        self._ancillary.write(data)
        frame = av.VideoFrame.from_ndarray(data.payload, format=self.format)
        self._encode(frame)

    def close(self):
        self._encode(None)
        self._frames.close()
        self._ancillary.close()


class wr_rm_depth:
    def __init__(self, path, name, mode):
        self.path = path
        self.name = name
        self.mode = mode

    def _add_buffer(self, name, buffer):
        file = tarfile.TarInfo(name)
        file.size = len(buffer)
        self._frames.addfile(file, io.BytesIO(initial_bytes=buffer))

    def _encode(self, payload):
        depth = cv2.imencode('.png', payload.depth)[1]
        ab = cv2.imencode('.png', payload.ab)[1]
        self._add_buffer('depth_{v}.png'.format(v=self._id), depth)
        self._add_buffer('ab_{v}.png'.format(v=self._id), ab)
        self._id += 1

    def open(self):
        self._ancillary = _wr_ancillary(os.path.join(self.path, 'rm_depth_' + self.name + '_ancillary.bin'), self.mode)
        self._frames = tarfile.open(os.path.join(self.path, 'rm_depth_' + self.name + '.tar'), 'w')
        self._id = 0
        
    def write(self, data):
        self._ancillary.write(data)
        self._encode(data.payload)
       
    def close(self):
        self._frames.close()
        self._ancillary.close()


class wr_rm_imu:
    def __init__(self, path, name, mode):
        self.path = path
        self.name = name
        self.mode = mode

    def open(self):
        self._writer = raw_writer()
        self._writer.open(os.path.join(self.path, 'rm_imu_' + self.name + '.bin'), self.mode)

    def write(self, data):
        self._writer.write(data)

    def close(self):
        self._writer.close()


class wr_pv:
    def __init__(self, path, mode, width, height, framerate, codec, bitrate, format):
        self.path = path
        self.mode = mode
        self.width = width
        self.height = height
        self.framerate = framerate
        self.codec = codec
        self.bitrate = bitrate
        self.format = format

    def _encode(self, frame):
        for packet in self._stream.encode(frame):
            self._frames.mux(packet)

    def open(self):        
        self._ancillary = _wr_ancillary(os.path.join(self.path, 'pv_ancillary.bin'), self.mode)
        self._frames = av.open(os.path.join(self.path, 'pv.mp4'), mode='w')
        self._stream = self._frames.add_stream(self.codec, rate=self.framerate)
        self._stream.width = self.width
        self._stream.height = self.height
        self._stream.pix_fmt = Parameters_PV.FORMAT
        self._stream.bit_rate = self.bitrate

    def write(self, data):
        self._ancillary.write(data)
        frame = av.VideoFrame.from_ndarray(data.payload, format=self.format)
        self._encode(frame)

    def close(self):
        self._encode(None)
        self._frames.close()
        self._ancillary.close()


class wr_mc:
    def __init__(self, path, profile):
        self.path = path
        self.profile = profile

    def _encode(self, frame):
        if (frame is not None):
            frame.sample_rate = Parameters_MC.SAMPLE_RATE
        for packet in self._stream.encode(frame):
            packet.pts = self._id
            packet.dts = self._id
            packet.time_base = Fraction(Parameters_MC.GROUP_SIZE, Parameters_MC.SAMPLE_RATE)
            self._frames.mux(packet)
            self._id += 1

    def open(self):        
        self._ancillary = _wr_ancillary(os.path.join(self.path, 'mc_ancillary.bin'), StreamMode.MODE_0)
        self._frames = av.open(os.path.join(self.path, 'mc.mp4'), mode='w', format=Parameters_MC.CONTAINER)
        self._stream = self._frames.add_stream(get_audio_codec_name(self.profile), rate=Parameters_MC.SAMPLE_RATE)
        self._stream.bit_rate = get_audio_codec_bitrate(self.profile)
        self._id = 0

    def write(self, data):
        self._ancillary.write(data)
        frame = av.AudioFrame.from_ndarray(data.payload, format=Parameters_MC.FORMAT, layout=Parameters_MC.LAYOUT)        
        self._encode(frame)
        
    def close(self):
        self._encode(None)
        self._frames.close()
        self._ancillary.close()


class wr_si:
    def __init__(self, path):
        self.path = path

    def open(self):
        self._writer = raw_writer()
        self._writer.open(os.path.join(self.path, 'si.bin'), StreamMode.MODE_0)

    def write(self, data):
        self._writer.write(data)

    def close(self):
        self._writer.close()


#------------------------------------------------------------------------------
# Readers
#------------------------------------------------------------------------------

class _rd_ancillary:
    def __init__(self, filename):
        self._file = open(filename, 'rb')
        self._mode = struct.unpack('<B', self._file.read(_SIZEOF.BYTE))[0]

    def read_field(self, size):
        data = self._file.read(size)
        if (len(data) != size):
            return None
        return data

    def read_timestamp(self):
        timestamp = self.read_field(_SIZEOF.LONGLONG)
        if (timestamp is None):
            return None
        return struct.unpack('<Q', timestamp)[0]

    def read_pose(self):
        pose = self.read_field(_SIZEOF.FLOAT * 16)
        if (pose is None):
            return None
        return np.frombuffer(pose, dtype=np.float32).reshape((4, 4))

    def assemble(self, payload):
        if (payload is None):
            return None
        timestamp = self.read_timestamp()
        if (timestamp is None):
            return None
        if (self._mode == StreamMode.MODE_1):
            pose = self.read_pose()
            if (pose is None):
                return None
        else:
            pose = None
        return packet(timestamp, payload, pose)

    def close(self):
        self._file.close()


class rd_rm_vlc:
    def __init__(self, path, name, format):
        self.path = path
        self.name = name
        self.format = format

    def _decode(self):
        payload = next(self._generator, None)
        if (payload is not None):
            payload = payload.to_ndarray(format=self.format)
        return payload

    def open(self):
        self._ancillary = _rd_ancillary(os.path.join(self.path, 'rm_vlc_' + self.name + '_ancillary.bin'))
        self._frames = av.open(os.path.join(self.path, 'rm_vlc_' + self.name + '.mp4'), mode='r')
        self._generator = (frame for frame in self._frames.decode(video=0))
        
    def read(self):
        return self._ancillary.assemble(self._decode())

    def close(self):
        self._frames.close()
        self._ancillary.close()


class rd_rm_depth:
    def __init__(self, path, name):
        self.path = path
        self.name = name

    def _decode(self):
        try:
            depth_file = self._frames.extractfile('depth_{v}.png'.format(v=self._id))
            ab_file = self._frames.extractfile('ab_{v}.png'.format(v=self._id))
        except KeyError:
            return None

        depth = cv2.imdecode(np.frombuffer(depth_file.read(), dtype=np.uint8), cv2.IMREAD_UNCHANGED)
        ab = cv2.imdecode(np.frombuffer(ab_file.read(), dtype=np.uint8), cv2.IMREAD_UNCHANGED)
        self._id += 1

        return RM_Depth_Frame(depth, ab)

    def open(self):
        self._ancillary = _rd_ancillary(os.path.join(self.path, 'rm_depth_' + self.name + '_ancillary.bin'))
        self._frames = tarfile.open(os.path.join(self.path, 'rm_depth_' + self.name + '.tar'), 'r')
        self._id = 0        
  
    def read(self):
        return self._ancillary.assemble(self._decode())

    def close(self):
        self._frames.close()
        self._ancillary.close()


class rd_rm_imu:
    def __init__(self, path, name, chunk_size):
        self.path = path
        self.name = name
        self.chunk_size = chunk_size

    def open(self):
        self._reader = raw_reader()
        self._reader.open(os.path.join(self.path, 'rm_imu_' + self.name + '.bin'), self.chunk_size)
        
    def read(self):
        return self._reader.read()

    def close(self):
        self._reader.close()


class rd_pv:
    def __init__(self, path, format):
        self.path = path
        self.format = format

    def _decode(self):
        payload = next(self._generator, None)
        if (payload is not None):
            payload = payload.to_ndarray(format=self.format)
        return payload

    def open(self):
        self._ancillary = _rd_ancillary(os.path.join(self.path, 'pv_ancillary.bin'))        
        self._frames = av.open(os.path.join(self.path, 'pv.mp4'), mode='r')
        self._generator = (frame for frame in self._frames.decode(video=0))        

    def read(self):
        return self._ancillary.assemble(self._decode())

    def close(self):
        self._frames.close()
        self._ancillary.close()


class rd_mc:
    def __init__(self, path):
        self.path = path

    def _decode(self):
        payload = next(self._generator, None)
        if (payload is not None):
            payload = payload.to_ndarray()
        return payload

    def open(self):
        self._ancillary = _rd_ancillary(os.path.join(self.path, 'mc_ancillary.bin'))
        self._frames = av.open(os.path.join(self.path, 'mc.mp4'), mode='r')
        self._generator = (frame for frame in self._frames.decode(audio=0))
        next(self._generator)

    def read(self):
        return self._ancillary.assemble(self._decode())

    def close(self):
        self._frames.close()
        self._ancillary.close()


class rd_si:
    def __init__(self, path, chunk_size):
        self.path = path
        self.chunk_size = chunk_size

    def open(self):
        self._reader = raw_reader()
        self._reader.open(os.path.join(self.path, 'si.bin'), self.chunk_size)
        
    def read(self):
        return self._reader.read()
        
    def close(self):
        self._reader.close()


#------------------------------------------------------------------------------
# Utilities
#------------------------------------------------------------------------------

class pose_printer:
    def __init__(self, period):
        self._period = period
        self._count = 0

    def push(self, timestamp, pose):
        self._count += 1
        if (self._count >= self._period):
            self._count = 0
            if (pose is not None):
                print('Pose at time {ts}'.format(ts=timestamp))
                print(pose)


class framerate_counter:
    def __init__(self, period):
        self._period = period
        self._count = 0
        self._start = None

    def push(self):
        if (self._start is None):
            self._start = time.perf_counter()
        else:
            self._count += 1
            if (self._count >= self._period):
                ts = time.perf_counter()
                fps = self._count / (ts - self._start)
                print('FPS: {fps}'.format(fps=fps))
                self._count = 0
                self._start = ts


class continuity_analyzer:
    def __init__(self, period):
        self._last = None
        self._period = period

    def push(self, timestamp):
        ret = 0
        if (self._last is not None):
            delta = timestamp - self._last
            if (delta > (1.5 * self._period)):
                ret = 1
            elif (delta < (0.5 * self._period)):
                ret = -1
        self._last = timestamp
        return ret

