## Public Transport

This project publishes public transport data from around 
the world. It aggregates GTFS Realtime feeds from various
cities and makes them available via MQTT. The original GTFS RT
Protobuf format is used so the published message are in
binary form.

It also extracts vehicle positions from the GTFS Realtime feeds
and publishes the positions on a topic per vehicle.

The topics are as follows:

- `stefan/public-transport/{operator}/vehicle_positions/{vehicle_id}`
- `stefan/public-transport/{operator}/pb`

