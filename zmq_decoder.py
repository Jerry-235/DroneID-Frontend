#!/usr/bin/env python3
# (c) 2024 B.Kerler
import json
import sys
import time
import zmq
import argparse
import serial
from threading import Thread
from OpenDroneID.decoder import decode_ble, decode
from OpenDroneID.utils import structhelper_io

verbose = False  # Global variable to control verbosity
stop = False
UART_MAX_BUFFER = 65536  # a partial object longer than this is noise; drop it


def log(*msg):
    """Logs messages to stderr if verbose is enabled."""
    global verbose
    if verbose:
        s = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"{s}:", *msg, end="\n", file=sys.stderr)


def zmq_thread(pub_socket):
    """Handles ZMQ subscriber notifications. Polls rather than blocking on
    recv, so shutdown can join this thread before the socket is closed."""
    global stop
    poller = zmq.Poller()
    poller.register(pub_socket, zmq.POLLIN)
    while not stop:
        try:
            if pub_socket not in dict(poller.poll(500)):
                continue
            event = pub_socket.recv()
        except zmq.error.ContextTerminated:
            break
        except zmq.ZMQError as e:
            log("ZMQ Thread Error:", e)
            continue
        if verbose and event:
            if event[0] == 1:
                log("New subscriber for", event[1:])
            elif event[0] == 0:
                log("Unsubscribed", event[1:])


def decoder_thread(socket, pub):
    """Handles Bluetooth/Wi-Fi OpenDroneID messages."""
    global stop
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    try:
        while not stop:
            socks = dict(poller.poll(3000))  # Poll every 3 seconds
            if socket in socks and socks[socket] == zmq.POLLIN:
                try:
                    data = socket.recv()
                except zmq.error.ZMQError as e:
                    if verbose:
                        log("Receive Error:", e)
                    continue

                if data:
                    try:
                        dc = json.loads(data.decode('utf-8'))
                    except json.JSONDecodeError as e:
                        if verbose:
                            log("JSON Decode Error:", e)
                        continue

                    if verbose:
                        print("ZMQ Data Received:", json.dumps(dc, indent=2))

                    # Process Bluetooth/Wi-Fi data. A message that fails to
                    # decode is skipped; it must not end this thread, which
                    # would silently stop every later detection.
                    try:
                        process_decoded_data(dc, pub)
                    except Exception as e:
                        if verbose:
                            log("Skipped a message that did not decode:", e)
    except zmq.error.ContextTerminated:
        pass
    except Exception as e:
        if verbose:
            log("Decoder Thread Error:", e)
    finally:
        # Closed here, on the thread that owns it, so the context can terminate.
        socket.close(linger=0)


def split_json_objects(buf: str):
    """Pull complete top-level JSON objects out of a UART byte stream.

    Brace counting alone breaks on a '{' or '}' inside a string value, so this
    scanner tracks string state too. Returns (objects, remainder): remainder is
    an unfinished object (kept for the next read) or "" when there is none.
    """
    objects = []
    depth = 0
    in_str = False
    esc = False
    start = None
    for i, ch in enumerate(buf):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}" and depth > 0:
            depth -= 1
            if depth == 0:
                try:
                    objects.append(json.loads(buf[start:i + 1]))
                except json.JSONDecodeError as e:
                    log("UART JSON Decode Error:", e)
                start = None
    if depth == 0:
        return objects, ""
    remainder = buf[start:]
    if len(remainder) > UART_MAX_BUFFER:
        remainder = ""
    return objects, remainder


def uart_listener(uart_device, pub):
    """Reads ESP32 UART data and forwards it via ZMQ, with automatic reconnect."""
    global stop
    buffer = ""

    log(f"UART listener starting for {uart_device}")

    while not stop:
        try:
            log(f"Attempting UART connection to {uart_device}...")
            with serial.Serial(uart_device, baudrate=115200, timeout=1) as ser:
                log(f"UART connected: {uart_device}")
                buffer = ""

                # Inner read loop
                while not stop:
                    if ser.in_waiting > 0:
                        try:
                            data = ser.read(ser.in_waiting).decode('utf-8', errors='replace')
                            if verbose:
                                print("UART received:", data)
                            objects, buffer = split_json_objects(buffer + data)
                            for dc in objects:
                                json_data = json.dumps(dc)
                                if pub:
                                    pub.send_string(json_data)
                                if verbose:
                                    print(f"Forwarded via ZMQ: {json_data}")

                        except Exception as e:
                            log("UART Read Error:", e)
                    else:
                        time.sleep(0.1)

        except serial.SerialException as e:
            log(f"UART connect error ({uart_device}): {e}")
            time.sleep(2)  # retry delay
        except Exception as e:
            log(f"Unexpected UART error: {e}")
            time.sleep(2)


def dji_listener(dji_url, pub):
    """Subscribes to DJI Receiver and forwards data as-is."""
    global stop
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.SUBSCRIBE, b'')  # Subscribe to all messages
    try:
        socket.connect(f"tcp://{dji_url}")
        log(f"Connected to DJI Receiver at {dji_url}")
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        while not stop:
            socks = dict(poller.poll(3000))  # Poll every 3 seconds
            if socket in socks and socks[socket] == zmq.POLLIN:
                try:
                    data = socket.recv_string()
                    if pub:
                        pub.send_string(data)  # Forward raw DJI data
                    if verbose:
                        log(f"DJI Data Forwarded: {data}")
                except zmq.ZMQError as e:
                    log("DJI ZMQ Error:", e)
                    continue
    except zmq.error.ZMQError as e:
        log(f"Error connecting to DJI Receiver at {dji_url}: {e}")
    finally:
        socket.close()
        context.term()


def tag_transport(messages, transport):
    """Stamp each decoded message with the radio it was heard on ("wifi" or
    "bluetooth"), so the master can say which sensor saw a drone."""
    for msg in messages:
        if isinstance(msg, dict):
            msg["transport"] = transport
    return messages


def tag_transport_json(json_data, transport):
    """tag_transport for a JSON array string. Anything that isn't a JSON array
    is passed through unchanged."""
    try:
        parsed = json.loads(json_data)
    except (TypeError, ValueError):
        return json_data
    if not isinstance(parsed, list):
        return json_data
    return json.dumps(tag_transport(parsed, transport))


def process_decoded_data(dc, pub):
    """Processes and forwards the decoded Bluetooth/Wi-Fi data."""
    # --- BLE path ---
    if "AUX_ADV_IND" in dc and "aa" in dc["AUX_ADV_IND"] and dc["AUX_ADV_IND"]["aa"] == 0x8e89bed6:
        if "AdvData" in dc:
            try:
                advdata = bytearray(bytes.fromhex(dc["AdvData"]))
                if advdata[1] == 0x16 and int.from_bytes(advdata[2:4], 'little') == 0xFFFA and advdata[4] == 0x0D:
                    if verbose:
                        print("Open Drone ID BT4/BT5\n-------------------------\n")
                    json_data = decode_ble(advdata)

                    # Add AdvA address to JSON if available
                    if "aext" in dc and "AdvA" in dc["aext"]:
                        try:
                            json_obj = json.loads(json_data)
                            if isinstance(json_obj, list) and len(json_obj) > 0:
                                for msg in json_obj:
                                    if "Basic ID" in msg:
                                        adv_a = dc["aext"]["AdvA"].split()[0]
                                        msg["Basic ID"]["MAC"] = adv_a
                            json_data = json.dumps(json_obj)
                        except json.JSONDecodeError:
                            pass

                    # Radio details for the track, same fields the Wi-Fi path carries:
                    # RSSI and the BLE channel Sniffle reported for the packet.
                    aux = dc["AUX_ADV_IND"]
                    radio = {k: aux[src] for k, src in (("RSSI", "rssi"), ("channel", "chan")) if src in aux}
                    if radio:
                        try:
                            json_obj = json.loads(json_data)
                            if isinstance(json_obj, list):
                                for msg in json_obj:
                                    msg.update(radio)
                                json_data = json.dumps(json_obj)
                        except json.JSONDecodeError:
                            pass

                    json_data = tag_transport_json(json_data, "bluetooth")
                    if pub:
                        pub.send_string(json_data)
                    if verbose:
                        print(json_data)
                        print()
                    sys.stdout.flush()
            except ValueError as e:
                log("AdvData Decode Error:", e)

    # --- Wi-Fi path ---
    elif "DroneID" in dc:
        for mac, field in dc["DroneID"].items():
            if verbose:
                print("Open Drone ID WIFI\n-------------------------\n")

            merged = []  # decoded messages, published as one list

            # If we have raw AdvData, decode into multiple messages
            if "AdvData" in field:
                try:
                    fields = decode(structhelper_io(bytes.fromhex(field["AdvData"])))
                    for field_decoded in fields:
                        field_decoded["MAC"] = mac
                        # Wi-Fi radio info (RSSI, channel, freq) from the wifi receiver
                        for key in ("RSSI", "channel", "freq"):
                            if key in field:
                                field_decoded[key] = field[key]
                        # Add RSSI to decoded fields if available (Bluetooth)
                        if "AUX_ADV_IND" in dc and "RSSI" not in field_decoded:
                            field_decoded["RSSI"] = dc["AUX_ADV_IND"]["rssi"]
                        merged.append(field_decoded)
                except Exception as e:
                    log("Decoding Error:", e)
            else:
                # No AdvData: still emit a list with a minimal entry
                entry = {"MAC": mac}
                for key in ("RSSI", "channel", "freq"):
                    if key in field:
                        entry[key] = field[key]
                if "AUX_ADV_IND" in dc and "RSSI" not in entry:
                    entry["RSSI"] = dc["AUX_ADV_IND"]["rssi"]
                # If a pre-parsed structure is already in 'field', keep it alongside MAC/RSSI
                # but ensure we output a list for consistency with BLE
                if field:
                    try:
                        # shallow copy to avoid mutating original
                        base = dict(field)
                        base["MAC"] = entry["MAC"]
                        if "RSSI" in entry:
                            base["RSSI"] = entry["RSSI"]
                        merged.append(base)
                    except Exception:
                        merged.append(entry)
                else:
                    merged.append(entry)

            # Publish once as a JSON array
            if merged:
                json_data = json.dumps(tag_transport(merged, "wifi"))
                if pub:
                    pub.send_string(json_data)
                if verbose:
                    print(json_data)
                print() if verbose else None
            sys.stdout.flush()


def main():
    global stop, verbose
    info = "ZMQ decoder for BLE4/5 + WIFI + DJI ZMQ clients (c) B.Kerler 2024-2025"
    aparse = argparse.ArgumentParser(description=info)
    aparse.add_argument("-z", "--zmq", action="store_true", help="Enable ZMQ")
    aparse.add_argument("-v", "--verbose", action="store_true", help="Print decoded messages")
    aparse.add_argument("--zmqsetting", default="127.0.0.1:4224", help="Define ZMQ server settings")
    aparse.add_argument("--zmqclients", default="127.0.0.1:4222,127.0.0.1:4223",
                        help="Define Bluetooth/Wi-Fi ZMQ clients")
    aparse.add_argument("--uart", help="UART device for pre-decoded ESP32 data (e.g., /dev/ttyACM0)")
    aparse.add_argument("--dji", help="DJI receiver ZMQ endpoint (e.g., 127.0.0.1:4221)")
    args = aparse.parse_args()
    if (args.uart or args.dji) and not args.zmq:
        # Their output is only published over ZMQ; without -z it would be dropped.
        aparse.error("--uart and --dji need -z: their data is only published over ZMQ")

    verbose = args.verbose

    # Initialize a single ZMQ context
    sctx = zmq.Context()

    if args.zmq:
        pub = sctx.socket(zmq.XPUB)
        pub.setsockopt(zmq.XPUB_VERBOSE, True)
        purl = f"tcp://{args.zmqsetting}"
        pub.bind(purl)

        zthread = Thread(target=zmq_thread, args=(pub,), daemon=True, name='zmq')
        zthread.start()
    else:
        pub = None
        zthread = None

    # Set up UART listener
    uart_thread = None
    dji_thread = None
    if args.uart:
        uart_thread = Thread(target=uart_listener, args=(args.uart, pub), daemon=True)
        uart_thread.start()

    # Set up DJI listener
    if args.dji:
        dji_thread = Thread(target=dji_listener, args=(args.dji, pub), daemon=True, name='dji')
        dji_thread.start()

    # Set up ZMQ client listeners
    clients = args.zmqclients.split(",")
    subs = []
    for client in clients:
        url = f"tcp://{client}"
        sub = sctx.socket(zmq.SUB)
        sub.setsockopt(zmq.SUBSCRIBE, b'{"AUX_ADV_IND"')
        sub.setsockopt(zmq.SUBSCRIBE, b'{"DroneID"')
        try:
            sub.connect(url)
        except zmq.error.ZMQError as e:
            log(f"Failed to connect to {url}: {e}")
            continue

        dthread = Thread(target=decoder_thread, args=(sub, pub), daemon=True, name=f'decoder-{client}')
        dthread.start()
        subs.append(dthread)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("Interrupt received, shutting down...")
    finally:
        # Threads stop on their own within a poll interval; join them before
        # the context is terminated, since term() waits for open sockets.
        stop = True
        for thread in subs:
            thread.join(timeout=5)
        for thread in (uart_thread, dji_thread, zthread):
            if thread is not None:
                thread.join(timeout=5)
        if pub:
            pub.close(linger=0)
        sctx.term()
        log("Shutdown complete.")


if __name__ == "__main__":
    main()