import uuid
import random
import time
from datetime import datetime, timedelta
from faker import Faker
from geopy.distance import geodesic
from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer
import io

#Azure connection with the two eventhubs for the two data feeds
EVENT_HUB_NAMESPACE = "iesstsabbadbaa-grp-06-10.servicebus.windows.net:9093"
RIDE_TOPIC  = "ride-requests-g8-v2"
DRIVER_TOPIC = "drivers-g8-v2"
VERBOSE = True

# Ride‑requests producer
rides_producer_config = {
    "bootstrap.servers": EVENT_HUB_NAMESPACE,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":  "PLAIN",
    "sasl.username":    "$ConnectionString",
    "sasl.password":    (
        "Endpoint=sb://iesstsabbadbaa-grp-06-10.servicebus.windows.net/;"
        "SharedAccessKeyName=ridesproducer;"
        "SharedAccessKey=j0facr/QcwtgxHjy/YmhUAiMmenl81ir/+AEhN17enw=;"
        "EntityPath=ride-requests-g8-v2"
    )
}
rides_producer = Producer(rides_producer_config)

# Driver‑updates producer
drivers_producer_config = {
    "bootstrap.servers": EVENT_HUB_NAMESPACE,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":  "PLAIN",
    "sasl.username":    "$ConnectionString",
    "sasl.password":    (
        "Endpoint=sb://iesstsabbadbaa-grp-06-10.servicebus.windows.net/;"
        "SharedAccessKeyName=driversproducer;"
        "SharedAccessKey=kPVuOGO+4OXXJLYYN85KaKRkW8P1+IDsI+AEhLKsYEk=;"
        "EntityPath=drivers-g8-v2"
    )
}
drivers_producer = Producer(drivers_producer_config)

# Locale & payment
fake = Faker('es_ES')
VEHICLE_TYPES = {"Standard":1.0, "XL":1.2, "Electric":1.5, "Luxury":2.0}
PAYMENT_METHODS      = ["Credit Card","Cash","Digital Wallet","Voucher"]
PAYMENT_PROBABILITIES = [0.15, 0.10, 0.7, 0.05]

# Avro Schemas

ride_request_schema = parse_schema({
    "type":"record","name":"RideRequest",
    "fields":[
        {"name":"request_id",  "type":"string"},
        {"name":"passenger_id","type":"string"},
        {"name":"passenger_name","type":"string"},
        {"name":"driver_name", "type":["null","string"]},
        {"name":"pickup_latitude","type":"double"},
        {"name":"pickup_longitude","type":"double"},
        {"name":"destination_latitude","type":"double"},
        {"name":"destination_longitude","type":"double"},
        {"name":"timestamp",   "type":"string"},
        {"name":"vehicle_type","type":"string"},
        {"name":"surge_multiplier","type":"float"},
        {"name":"traffic_delay_multiplier","type":"float"},
        {"name":"pickup_distance_km","type":"float"},
        {"name":"pickup_eta_min","type":"float"},
        {"name":"distance_km","type":"float"},
        {"name":"estimated_ride_time_min","type":"float"},
        {"name":"fare_euros","type":"float"},
        {"name":"payment_method","type":"string"},
        {"name":"driver_rating_given","type":"float"}
    ]
})

driver_profile_schema = parse_schema({
    "type":"record","name":"DriverProfile",
    "fields":[
        {"name":"driver_id",  "type":"string"},
        {"name":"driver_name","type":"string"},
        {"name":"vehicle_type","type":"string"},
        {"name":"latitude",   "type":"double"},
        {"name":"longitude",  "type":"double"},
        {"name":"status",     "type":"string"},
        {"name":"shift_start","type":"int"},
        {"name":"shift_duration","type":"int"}
    ]
})

# Realism Functions

def random_madrid_location():
    lat = round(random.uniform(40.40, 40.44),6)
    lon = round(random.uniform(-3.72,-3.67),6)
    return {"latitude":lat,"longitude":lon}

def get_traffic_level(hour):
    if 7<=hour<=9 or 17<=hour<=20:
        return round(random.uniform(1.4,2.0),2)
    if 12<=hour<=14:
        return round(random.uniform(1.2,1.4),2)
    return round(random.uniform(1.0,1.1),2)

def get_surge_multiplier(demand,supply,hour):
    ratio = demand/supply if supply>0 else 5.0
    if 7<=hour<=10 or 17<=hour<=21:
        base = 1.3
    elif hour>=22 or hour<=2:
        base = 1.8
    else:
        base = 1.0
    return round(min(base + (ratio-1.0)*random.uniform(0.5,1.5),3.5),2)

def estimate_ride_time(dist_km,traffic):
    return round((dist_km/30)*traffic*60,1)

def calculate_fare(dist_km,vehicle,surge,pickup_km,ride_time,hour):
    base = 3.0 if 7<=hour<=21 else 2.5
    per_km=1.2
    pickup_fee=pickup_km
    time_cost=ride_time*0.30
    if vehicle=="Luxury": base+=2.0
    elif vehicle=="Electric": time_cost*=0.95
    dist_cost = dist_km*per_km*surge*VEHICLE_TYPES[vehicle]
    return round(max(base+dist_cost+pickup_fee+time_cost,4.0),2)

def send_ride_event(ev):
    buf = io.BytesIO()
    schemaless_writer(buf, ride_request_schema, ev)
    rides_producer.produce(RIDE_TOPIC, buf.getvalue())

def send_driver_event(ev):
    buf = io.BytesIO()
    schemaless_writer(buf, driver_profile_schema, ev)
    drivers_producer.produce(DRIVER_TOPIC, buf.getvalue())

# Simulation:

if __name__=="__main__":
    # generate 30 drivers
    drivers = []
    for _ in range(30):
        loc = random_madrid_location()
        drivers.append({
            "driver_id":str(uuid.uuid4()),
            "driver_name":fake.name(),
            "vehicle_type":random.choice(list(VEHICLE_TYPES)),
            "latitude":loc["latitude"],
            "longitude":loc["longitude"],
            "status":"available",
            "shift_start":random.choice([0,6,12,18]),
            "shift_duration":8
        })

    start = datetime.strptime("2025-04-14 00:00","%Y-%m-%d %H:%M")
    end   = start + timedelta(hours=24)
    step  = timedelta(minutes=5)
    t     = start
    step_i=0

    print("🚀 Starting real-time simulation…")
    while t<end:
        h = t.hour
        traffic = get_traffic_level(h)
        demand  = random.randint(80,120)
        supply  = sum(1 for d in drivers if d["status"]=="available")
        surge   = get_surge_multiplier(demand,supply,h)

        # choose how many rides this 5‑min interval
        if 6<=h<=9 or 17<=h<=20:
            ride_ct = random.randint(6,10)
        elif h>=22 or h<=1:
            ride_ct = random.randint(4,7)
        elif 12<=h<=14:
            ride_ct = random.randint(3,6)
        else:
            ride_ct = random.randint(1,3)

        for _ in range(ride_ct):
            p = random_madrid_location()
            d = random_madrid_location()
            veh = random.choice(list(VEHICLE_TYPES))
            drv = random.choice(drivers)

            pick_km = geodesic((p["latitude"],p["longitude"]),
                               (drv["latitude"],drv["longitude"])).km
            trip_km = geodesic((p["latitude"],p["longitude"]),
                               (d["latitude"],d["longitude"])).km

            ride_time = estimate_ride_time(trip_km,traffic)
            eta       = estimate_ride_time(pick_km,1.0)
            fare      = calculate_fare(trip_km,veh,surge,pick_km,ride_time,h)
            rating    = min(max(round(random.normalvariate(4.6,0.3),2),3.0),5.0)

            ev = {
              "request_id":str(uuid.uuid4()),
              "passenger_id":str(uuid.uuid4()),
              "passenger_name":fake.name(),
              "driver_name":drv["driver_name"],
              "pickup_latitude":p["latitude"],
              "pickup_longitude":p["longitude"],
              "destination_latitude":d["latitude"],
              "destination_longitude":d["longitude"],
              "timestamp":t.isoformat(),
              "vehicle_type":veh,
              "surge_multiplier":surge,
              "traffic_delay_multiplier":traffic,
              "pickup_distance_km":round(pick_km,2),
              "pickup_eta_min":round(eta,1),
              "distance_km":round(trip_km,2),
              "estimated_ride_time_min":round(ride_time,1),
              "fare_euros":fare,
              "payment_method":random.choices(PAYMENT_METHODS,weights=PAYMENT_PROBABILITIES)[0],
              "driver_rating_given":rating
            }
            send_ride_event(ev)
            if VERBOSE:
                print(f"[{t.strftime('%H:%M')}] 🚕 Sent ride: {fare}€ | {ev['passenger_name']}")

        # every 3 steps (≈15 min) send driver positions
        if step_i % 3 == 0:
            for drv in drivers:
                loc = random_madrid_location()
                drv["latitude"], drv["longitude"] = loc["latitude"], loc["longitude"]
                send_driver_event(drv)
                if VERBOSE:
                    print(f"[{t.strftime('%H:%M')}] 🧍 Sent driver update: {drv['driver_name']}")

        t += step
        step_i += 1
        time.sleep(0.5)

    rides_producer.flush()
    drivers_producer.flush()
    print("✅ Simulation complete.")
