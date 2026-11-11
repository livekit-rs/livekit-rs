# Original publisher identity lifecycle

Source-only acceptance profile. Uses unchanged LiveKit browser SDK2.23.0 and server SDK2.19.2 through actual WebRTC and public RoomService. It publishes a real WebAudio microphone, demands20 actual outbound RTP packets and the actual server publication, unpublishes the last media track, then checks sticky ParticipantInfo.isPublisher. Public UpdateParticipant revokes/restores canPublish while no media tracks remain; public Room.numPublishers must follow0→1. A fresh room and new ParticipantSID must start nonpublisher. Original owned browser child exit0, contexts0, stderr hash/identity, Node/server exit0 and all port/process cleanup gates remain in the existing runner.

Mac coordinator can select the new explicit profile with existing required inputs:
`python3 tests/interop/standalone-policies/run-owned.py --profile publisher-lifecycle --side all ...`
Linux coordinator may use the existing owned VM/netem runner with its unchanged strict network restore/ownership gates:
`python3 tests/interop/standalone-policies/run-owned-linux-netem.py --profile publisher-lifecycle --side all ...`
The default `all` still covers the prior original two profiles; this new profile is explicit to avoid silently changing existing matrix sizes. No SDK modification or protocol simulation occurs. This script has been syntax checked only, not executed by the producer.
