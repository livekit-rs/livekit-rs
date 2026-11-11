// Fixed official protocol/auth/Twirp clients, unchanged wire and worker messages.
// Acceptance tool only: no server implementation or SDK patches.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/proto"
)

type bearer struct{ token string }

func (b bearer) RoundTrip(req *http.Request) (*http.Response, error) {
	copy := req.Clone(req.Context())
	copy.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(copy)
}

type worker struct {
	conn         *websocket.Conn
	mu           sync.Mutex
	registration *livekit.RegisterWorkerResponse
	events       chan *livekit.ServerMessage
	failures     chan error
	stop         chan struct{}
	done         chan struct{}
	identity     string
}

func (w *worker) send(message *livekit.WorkerMessage) error {
	data, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return w.conn.WriteMessage(websocket.BinaryMessage, data)
}
func read(conn *websocket.Conn) (*livekit.ServerMessage, error) {
	kind, data, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if kind != websocket.BinaryMessage {
		return nil, fmt.Errorf("worker response must use official binary protobuf framing: %d", kind)
	}
	msg := &livekit.ServerMessage{}
	err = proto.Unmarshal(data, msg)
	return msg, err
}
func connect(ctx context.Context, base, key, secret, agent, namespace, role string, load float32) (*worker, error) {
	token, err := auth.NewAccessToken(key, secret).SetIdentity("probe-" + role).SetVideoGrant(&auth.VideoGrant{Agent: true}).SetValidFor(2 * time.Minute).ToJWT()
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "http" {
		u.Scheme = "ws"
	} else if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		return nil, errors.New("node URLs require http or https")
	}
	u.Path = "/agent"
	u.RawQuery = "protocol=1"
	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+token)
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), headers)
	if err != nil {
		return nil, err
	}
	w := &worker{conn: conn, events: make(chan *livekit.ServerMessage, 32), failures: make(chan error, 1), stop: make(chan struct{}), done: make(chan struct{}), identity: "probe-" + role + "-" + agent}
	ok := false
	defer func() {
		if !ok {
			conn.Close()
		}
	}()
	conn.SetReadLimit(1 << 20)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err = w.send(&livekit.WorkerMessage{Message: &livekit.WorkerMessage_Register{Register: &livekit.RegisterWorkerRequest{Type: livekit.JobType_JT_ROOM, AgentName: agent, Namespace: &namespace, Deployment: "production-1", Version: "fixed-protocol-agentjob-probe", PingInterval: 1}}}); err != nil {
		return nil, err
	}
	msg, err := read(conn)
	if err != nil {
		return nil, err
	}
	w.registration = msg.GetRegister()
	if w.registration == nil || w.registration.WorkerId == "" || w.registration.GetServerInfo().GetNodeId() == "" {
		return nil, errors.New("actual worker registration/node identity missing")
	}
	status := livekit.WorkerStatus_WS_AVAILABLE
	if err = w.send(&livekit.WorkerMessage{Message: &livekit.WorkerMessage_UpdateWorker{UpdateWorker: &livekit.UpdateWorkerStatus{Load: load, Status: &status}}}); err != nil {
		return nil, err
	}
	// FIFO Ping/Pong barrier proves this real connection's load update was read.
	stamp := time.Now().UnixMilli()
	if err = w.send(&livekit.WorkerMessage{Message: &livekit.WorkerMessage_Ping{Ping: &livekit.WorkerPing{Timestamp: stamp}}}); err != nil {
		return nil, err
	}
	msg, err = read(conn)
	if err != nil {
		return nil, err
	}
	if msg.GetPong() == nil || msg.GetPong().GetLastTimestamp() != stamp {
		return nil, errors.New("load-update Ping/Pong barrier missing")
	}
	_ = conn.SetReadDeadline(time.Time{})
	go func() {
		defer close(w.done)
		for {
			msg, err := read(conn)
			if err != nil {
				select {
				case w.failures <- err:
				default:
				}
				return
			}
			if availability := msg.GetAvailability(); availability != nil {
				if availability.Job == nil {
					select {
					case w.failures <- errors.New("availability has no real job"):
					default:
					}
					return
				}
				err = w.send(&livekit.WorkerMessage{Message: &livekit.WorkerMessage_Availability{Availability: &livekit.AvailabilityResponse{JobId: availability.Job.Id, Available: true, ParticipantIdentity: w.identity, ParticipantName: "paired official protocol worker"}}})
				if err != nil {
					select {
					case w.failures <- err:
					default:
					}
					return
				}
			}
			if msg.GetPong() == nil {
				select {
				case w.events <- msg:
				case <-w.stop:
					return
				}
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-ticker.C:
				if err := w.send(&livekit.WorkerMessage{Message: &livekit.WorkerMessage_Ping{Ping: &livekit.WorkerPing{Timestamp: time.Now().UnixMilli()}}}); err != nil {
					return
				}
			}
		}
	}()
	ok = true
	return w, nil
}
func (w *worker) close() { close(w.stop); _ = w.conn.Close(); <-w.done }
func next(ctx context.Context, low, high *worker) (*livekit.ServerMessage, error) {
	select {
	case event := <-low.events:
		return event, nil
	case event := <-high.events:
		return nil, fmt.Errorf("higher-load node received losing availability/assignment: %T", event.Message)
	case err := <-low.failures:
		return nil, fmt.Errorf("remote worker read: %w", err)
	case err := <-high.failures:
		return nil, fmt.Errorf("local worker read: %w", err)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func run() (map[string]any, error) {
	nodeA := flag.String("node-a", "", "HTTP(S) room/API owner and high-load worker node")
	nodeB := flag.String("node-b", "", "HTTP(S) remote low-load worker node")
	label := flag.String("label", "", "source pair identity recorded by coordinator")
	flag.Parse()
	key, secret := os.Getenv("LIVEKIT_AGENT_API_KEY"), os.Getenv("LIVEKIT_AGENT_API_SECRET")
	if key == "" || secret == "" || *nodeA == "" || *nodeB == "" {
		return nil, errors.New("node-a/node-b and API credentials in LIVEKIT_AGENT_API_KEY/LIVEKIT_AGENT_API_SECRET are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	unique := fmt.Sprintf("pair-%d", time.Now().UnixNano())
	agent, namespace, room := unique, "ns-"+unique, "room-"+unique
	high, err := connect(ctx, *nodeA, key, secret, agent, namespace, "high", .6)
	if err != nil {
		return nil, fmt.Errorf("high node register: %w", err)
	}
	defer high.close()
	low, err := connect(ctx, *nodeB, key, secret, agent, namespace, "low", .1)
	if err != nil {
		return nil, fmt.Errorf("low node register: %w", err)
	}
	defer low.close()
	ownerID, remoteID := high.registration.GetServerInfo().GetNodeId(), low.registration.GetServerInfo().GetNodeId()
	if ownerID == remoteID {
		return nil, errors.New("pair must be two distinct actual nodes")
	}
	apiToken, err := auth.NewAccessToken(key, secret).SetVideoGrant(&auth.VideoGrant{RoomCreate: true, RoomList: true, RoomAdmin: true, Room: room}).SetValidFor(2 * time.Minute).ToJWT()
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: bearer{apiToken}, Timeout: 10 * time.Second}
	rooms := livekit.NewRoomServiceProtobufClient(strings.TrimRight(*nodeA, "/"), client)
	dispatches := livekit.NewAgentDispatchServiceProtobufClient(strings.TrimRight(*nodeA, "/"), client)
	created, err := rooms.CreateRoom(ctx, &livekit.CreateRoomRequest{Name: room, NodeId: ownerID, EmptyTimeout: 60})
	if err != nil {
		return nil, fmt.Errorf("actual CreateRoom: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = rooms.DeleteRoom(cleanup, &livekit.DeleteRoomRequest{Room: room})
	}()
	if created.Name != room || created.Sid == "" {
		return nil, errors.New("created room identity missing")
	}
	dispatch, err := dispatches.CreateDispatch(ctx, &livekit.CreateAgentDispatchRequest{Room: room, AgentName: agent, Deployment: "production-1", Metadata: "real paired remote dispatch π", Attributes: map[string]string{"probe": "official-protocol"}})
	if err != nil {
		return nil, fmt.Errorf("actual CreateDispatch: %w", err)
	}
	if dispatch.Id == "" {
		return nil, errors.New("dispatch id missing")
	}
	offer, err := next(ctx, low, high)
	if err != nil {
		return nil, fmt.Errorf("remote availability: %w", err)
	}
	if offer.GetAvailability() == nil || offer.GetAvailability().Job == nil {
		return nil, errors.New("first event must be real availability")
	}
	assignmentMessage, err := next(ctx, low, high)
	if err != nil {
		return nil, fmt.Errorf("remote assignment: %w", err)
	}
	assignment := assignmentMessage.GetAssignment()
	if assignment == nil || assignment.Job == nil {
		return nil, errors.New("second event must be real assignment")
	}
	job := assignment.Job
	if job.Id != offer.GetAvailability().Job.Id || job.Room.GetName() != room || job.AgentName != agent || job.Namespace != namespace || job.Deployment != "production-1" || job.Metadata != "real paired remote dispatch π" || job.Attributes["probe"] != "official-protocol" {
		return nil, errors.New("assigned job differs from offered job/room/group")
	}
	if assignment.Url != nil {
		return nil, errors.New("fixed Go AssignJob omits optional URL; candidate must preserve worker connection URL fallback")
	}
	verifier, err := auth.ParseAPIToken(assignment.Token)
	if err != nil {
		return nil, err
	}
	if verifier.APIKey() != key {
		return nil, errors.New("assignment issuer mismatch")
	}
	_, grants, err := verifier.Verify(secret)
	if err != nil {
		return nil, fmt.Errorf("actual assignment token signature rejected: %w", err)
	}
	if grants.Identity != low.identity || grants.Video == nil || !grants.Video.RoomJoin || !grants.Video.Agent || grants.Video.Room != room {
		return nil, errors.New("assignment token actual identity/room/join/agent grants mismatch")
	}
	// ListDispatch must expose the actual remote worker's running state, not just
	// an accepted control request. No worker is migrated into the local registry.
	lastState := "no ListDispatch reply"
	for {
		listed, err := dispatches.ListDispatch(ctx, &livekit.ListAgentDispatchRequest{Room: room, DispatchId: dispatch.Id})
		if err != nil {
			return nil, fmt.Errorf("ListDispatch API: %w; last observed %s", err, lastState)
		}
		lastState = fmt.Sprintf("dispatches=%d", len(listed.AgentDispatches))
		ready := false
		for _, d := range listed.AgentDispatches {
			for _, j := range d.GetState().GetJobs() {
				lastState += fmt.Sprintf(" job=%s worker=%s status=%s", j.Id, j.GetState().GetWorkerId(), j.GetState().GetStatus())
				if j.Id == job.Id && j.GetState().GetWorkerId() == low.registration.WorkerId && j.GetState().GetStatus() == livekit.JobStatus_JS_RUNNING {
					ready = true
				}
			}
		}
		if ready {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("ListDispatch running state: %w; last observed %s", ctx.Err(), lastState)
		case <-time.After(25 * time.Millisecond):
		}
	}
	if _, err = dispatches.DeleteDispatch(ctx, &livekit.DeleteAgentDispatchRequest{Room: room, DispatchId: dispatch.Id}); err != nil {
		return nil, fmt.Errorf("actual DeleteDispatch: %w", err)
	}
	terminated, err := next(ctx, low, high)
	if err != nil {
		return nil, fmt.Errorf("remote JobTermination after DeleteDispatch: %w", err)
	}
	if terminated.GetTermination() == nil || terminated.GetTermination().JobId != job.Id {
		return nil, errors.New("remote owning worker did not receive actual JobTermination")
	}
	select {
	case event := <-high.events:
		return nil, fmt.Errorf("losing node unexpectedly received %T", event.Message)
	default:
	}
	if _, err = rooms.DeleteRoom(ctx, &livekit.DeleteRoomRequest{Room: room}); err != nil {
		return nil, fmt.Errorf("actual room resource cleanup: %w", err)
	}
	return map[string]any{"room_deleted": true, "accepted": true, "pair": *label, "owner_node": ownerID, "worker_node": remoteID, "worker_id": low.registration.WorkerId, "room": room, "dispatch_id": dispatch.Id, "job_id": job.Id, "assignment_token_signature": true, "assignment_url_absent": true, "low_load_claim_selected": true, "availability_assignment_running_termination": true}, nil
}
func main() {
	out, err := run()
	if err != nil {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"accepted": false, "error": err.Error()})
		os.Exit(1)
	}
	_ = json.NewEncoder(os.Stdout).Encode(out)
}
