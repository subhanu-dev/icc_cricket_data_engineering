import time
from datetime import datetime, timedelta, timezone
import random
import json
from azure.eventhub import EventHubProducerClient, EventData


# Connection details
connection_str = ""
eventhub_name = "streaminput"


# Define GMT+5:30 timezone
ist = timezone(timedelta(hours=5, minutes=30))


# Simulated data
team1_batsmen = [
    "Kohli",
    "Rohit",
    "Gill",
    "Pant",
    "Iyer",
    "Jadeja",
    "Ashwin",
    "Shami",
    "Siraj",
    "Kuldeep",
    "Bumrah",
]

team2_batsmen = [
    "Warner",
    "Smith",
    "Marsh",
    "Carey",
    "Maxwell",
    "Green",
    "Starc",
    "Zampa",
    "Cummins",
    "Hazlewood",
    "Abbott",
]
team1_bowlers = ["Cummins", "Starc", "Zampa", "Green", "Hazlewood"]
team2_bowlers = ["Bumrah", "Siraj", "Jadeja", "Hardik", "Ashwin"]

# Weighted event types (wicket is less likely)
event_types = [
    ("dot", 0),
    ("single", 1),
    ("double", 2),
    ("three", 3),
    ("boundary", 4),
    ("six", 6),
    ("wicket", 0),
]
event_weights = [25, 30, 15, 5, 10, 10, 5]  # Adjust to make "wicket" rare

# Initial game state
inning = 1
over, ball = 0, 1
current_batsmen = team1_batsmen.copy()
current_bowlers = team2_bowlers.copy()
on_strike = current_batsmen.pop(0)

# Azure Event Hub config
producer = EventHubProducerClient.from_connection_string(
    conn_str=connection_str, eventhub_name=eventhub_name
)

# Ball-by-ball loop
with producer:
    while True:
        bowler = random.choice(current_bowlers)
        event_type, runs = random.choices(event_types, weights=event_weights, k=1)[0]

        # Set IST timestamp in ISO format
        timestamp = datetime.now(ist).isoformat()

        # Prepare event
        event_payload = {
            "match_id": "icc_final_2025",
            "inning": inning,
            "over": over,
            "ball": ball,
            "batsman": on_strike,
            "bowler": bowler,
            "runs": runs,
            "event_type": event_type,
            "timestamp": timestamp,
        }

        # Send to Event Hub
        event_data = EventData(json.dumps(event_payload))
        producer.send_batch([event_data])
        print("Sent event:", json.dumps(event_payload, indent=2))

        # On wicket, get next batsman if available
        if event_type == "wicket":
            if current_batsmen:
                on_strike = current_batsmen.pop(0)
            else:
                print("All out!")
                break  # End innings

        # Update ball and over
        ball += 1
        if ball > 6:
            ball = 1
            over += 1

        # Switch innings after 20 overs
        if over == 20:
            inning += 1
            if inning > 2:
                print("Match over.")
                break

            over, ball = 0, 1
            current_batsmen = team2_batsmen.copy()
            current_bowlers = team1_bowlers.copy()
            on_strike = current_batsmen.pop(0)
            print("Switched innings!")

        time.sleep(15)
