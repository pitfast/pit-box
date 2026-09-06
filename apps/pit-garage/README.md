# PitFast Garage

pit-garage is a host-owned execution agent around one PitBox and one
Garage-local Grid. It registers an ephemeral session with a Circuit,
publishes heartbeat capacity and artifact locality, and accepts versioned
digest-pinned HTTP executions.

Example:

    pit-garage --id garage-a --listen 127.0.0.1:7101 \
      --circuit http://127.0.0.1:7090 \
      --pitlane http://127.0.0.1:7081 --lanes 8

The listener is infrastructure-owned and shared by all services. Services
never receive a listener, IP address, or dedicated lane. Remote artifact
warmup happens before PitBox scheduler admission and is singleflight per
digest.
