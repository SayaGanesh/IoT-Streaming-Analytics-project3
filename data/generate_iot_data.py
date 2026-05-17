"""
Generate sample IoT sensor payloads simulating factory floor equipment.
Output: NDJSON file mimicking Event Hub message bodies.
Devices: CNC machines, conveyor motors, hydraulic presses
"""

import json
import random
import uuid
from datetime import datetime, timedelta

random.seed(13)

DEVICE_IDS   = [f"DEVICE-{str(i).zfill(3)}" for i in range(1, 16)]
DEVICE_TYPES = {
    "DEVICE-001": "CNC_Machine",
    "DEVICE-002": "Conveyor_Motor",
    "DEVICE-003": "Hydraulic_Press",
    "DEVICE-004": "CNC_Machine",
    "DEVICE-005": "Conveyor_Motor",
}
for i in range(6, 16):
    t = ["CNC_Machine", "Conveyor_Motor", "Hydraulic_Press"][i % 3]
    DEVICE_TYPES[f"DEVICE-{str(i).zfill(3)}"] = t

FACTORY_ZONES = ["Zone-A", "Zone-B", "Zone-C", "Zone-D"]

def generate_payload(device_id, ts):
    device_type = DEVICE_TYPES.get(device_id, "Unknown")
    # Base sensor ranges per device type
    base_temp = {"CNC_Machine": 65, "Conveyor_Motor": 55, "Hydraulic_Press": 80}.get(device_type, 60)
    temp       = round(random.gauss(base_temp, 6), 3)
    vibration  = round(abs(random.gauss(1.8, 0.6)), 3)
    pressure   = round(random.gauss(3.5, 0.4), 3)

    # Inject anomalies
    if random.random() < 0.025:
        temp      = round(temp + random.uniform(20, 35), 3)   # thermal spike
    if random.random() < 0.015:
        vibration = round(vibration + random.uniform(3, 6), 3) # bearing fault

    return {
        "message_id":   str(uuid.uuid4()),
        "device_id":    device_id,
        "device_type":  device_type,
        "factory_zone": random.choice(FACTORY_ZONES),
        "timestamp":    ts.isoformat() + "Z",
        "temperature_c": temp,
        "vibration_ms2": vibration,
        "pressure_bar":  max(0, pressure),
        "rpm":           random.randint(800, 3200),
        "runtime_hours": round(random.uniform(0, 8760), 1),
        "firmware_ver":  "3.1.2",
        "schema_version": "v1"
    }

events = []
base_ts = datetime(2024, 3, 1, 8, 0, 0)

for device in DEVICE_IDS:
    t = base_ts
    for _ in range(120):  # 2 hours of readings per device, ~1 min apart
        events.append(generate_payload(device, t))
        t += timedelta(seconds=random.randint(55, 65))

random.shuffle(events)

with open("iot_sensor_payloads.json", "w") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")

print(f"Generated {len(events)} IoT sensor payloads from {len(DEVICE_IDS)} devices.")
print(f"Anomaly-injected: temperature spikes ~2.5%, vibration spikes ~1.5%")
