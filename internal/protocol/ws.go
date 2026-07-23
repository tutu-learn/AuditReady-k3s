package protocol

// WebSocket control-channel message types. The agent dials
// <endpoint>/k8s/ws (ws/wss matching the endpoint scheme) and authenticates
// with a first-frame hello; the server then pushes signed commands in real
// time and the agent streams execution reports back over the same socket.
// HTTP polling remains as the fallback path — see SERVER.md.

// WS frame type tags (snake_case, mirroring the host-agent tunnel protocol).
const (
	WsTypeAgentHello = "agent_hello" // agent → server, first frame
	WsTypeHelloAck   = "hello_ack"   // server → agent, auth result
	WsTypeCommand    = "command"     // server → agent, signed command push
	WsTypeReport     = "report"      // agent → server, execution report
	WsTypeError      = "error"       // server → agent, fatal or informational
)

// WsHello is the first frame the agent sends after the upgrade.
type WsHello struct {
	Type      string `json:"type"` // always WsTypeAgentHello
	Token     string `json:"token"`
	ClusterID string `json:"clusterId,omitempty"`
	Version   string `json:"version,omitempty"`
}

// WsHelloAck is the server's authentication reply.
type WsHelloAck struct {
	Type     string `json:"type"` // always WsTypeHelloAck
	Accepted bool   `json:"accepted"`
	Message  string `json:"message,omitempty"`
}

// WsEnvelope peeks at the type tag of an inbound frame.
type WsEnvelope struct {
	Type string `json:"type"`
}

// WsCommandMessage carries one signed command (same shape/signing as the
// HTTP poll response).
type WsCommandMessage struct {
	Type    string   `json:"type"` // always WsTypeCommand
	Command *Command `json:"command"`
}

// WsReportMessage carries one execution report.
type WsReportMessage struct {
	Type   string  `json:"type"` // always WsTypeReport
	Report *Report `json:"report"`
}

// WsErrorMessage is sent by the server before closing on a fatal condition,
// or for informational errors.
type WsErrorMessage struct {
	Type    string `json:"type"` // always WsTypeError
	Message string `json:"message"`
}
