from zk import ZK

# constant(s) for communicating with the zkteco biometric device
DEVICE_TIME_OUT = 10


class ZKTecoDevice:
    def __init__(self, device_ip, comm_port):
        self.device_ip = device_ip
        self.comm_port = comm_port

    def clear_records(self):
        clear_records_from_device(self.device_ip, self.comm_port)

    def pull_records(self):
        return pull_records_from_device(self.device_ip, self.comm_port)


def clear_records_from_device(device_ip, comm_port):
    """
    interface for communicating with the ZKTeco biometric device to
    clear all the attendance records from the machine
    """
    conn = None
    zk = ZK(
        device_ip,
        port=comm_port,
        timeout=DEVICE_TIME_OUT,
        force_udp=False,
        ommit_ping=False,
    )
    try:
        print("Connecting to device ...")
        conn = zk.connect()
        print("Disabling device ...")
        conn.disable_device()
        print("Firmware Version: : {}".format(conn.get_firmware_version()))

        print("deleting all attendance records stored on the biometric device")
        conn.clear_attendance()

        print("Enabling device ...")
        conn.enable_device()
    except Exception as e:
        print("Process terminate : {}".format(e))
    finally:
        if conn:
            conn.disconnect()


def pull_records_from_device(device_ip, comm_port):
    """
    interface for communicating with the ZKTeco biometric device to fetch
    the attendance records stored on the machine
    """
    conn = None
    zk = ZK(
        device_ip,
        port=comm_port,
        timeout=DEVICE_TIME_OUT,
        force_udp=False,
        ommit_ping=False,
    )
    try:
        users = []
        records = []

        print("Connecting to device ...")
        conn = zk.connect()
        print("Disabling device ...")
        conn.disable_device()
        print("Firmware Version: : {}".format(conn.get_firmware_version()))

        print("Fetching list of users...")
        users = conn.get_users()
        print("Fetching attendance records...")
        records = conn.get_attendance()

        print("Enabling device ...")
        conn.enable_device()
    except Exception as e:
        print("Process terminate : {}".format(e))
    finally:
        if conn:
            conn.disconnect()

        return users, records
