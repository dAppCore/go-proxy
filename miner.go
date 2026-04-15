package proxy

import (
	"crypto/tls"
	"net"
	"sync"
	"time"
)

// MinerState represents the lifecycle state of one miner connection.
//
//	WaitLogin → WaitReady → Ready → Closing
type MinerState int

const (
	MinerStateWaitLogin MinerState = iota // connection open, awaiting login request (10s timeout)
	MinerStateWaitReady                   // login validated, awaiting upstream pool job (600s timeout)
	MinerStateReady                       // receiving jobs, accepting submit requests
	MinerStateClosing                     // TCP close in progress
)

// Miner is the state machine for one miner TCP connection.
//
//	// created by Server on accept:
//	m := proxy.NewMiner(conn, 3333, nil)
//	m.Start()
type Miner struct {
	id                  int64  // monotonically increasing per-process; atomic assignment
	rpcID               string // UUID v4 sent to miner as session id
	state               MinerState
	extAlgo             bool // miner sent algo list in login params
	loginAlgos          []string
	extNH               bool   // NiceHash mode active (fixed byte splitting)
	algoEnabled         bool   // proxy is configured to negotiate the algo extension
	ip                  string // remote IP (without port, for logging)
	remoteAddr          string
	localPort           uint16
	user                string // login params.login (wallet address), custom diff suffix stripped
	password            string // login params.pass
	agent               string // login params.agent
	rigID               string // login params.rigid (optional extension)
	fixedByte           uint8  // NiceHash slot index (0-255)
	mapperID            int64  // which NonceMapper owns this miner; -1 = unassigned
	routeID             int64  // SimpleMapper ID in simple mode; -1 = unassigned
	customDiff          uint64 // 0 = use pool diff; non-zero = cap diff to this value
	customDiffResolved  bool
	customDiffFromLogin bool
	accessPassword      string
	globalDiff          uint64
	diff                uint64 // last difficulty sent to this miner from the pool
	rx                  uint64 // bytes received from miner
	tx                  uint64 // bytes sent from miner
	currentJob          Job
	connectedAt         time.Time
	lastActivityAt      time.Time
	mu                  sync.RWMutex
	conn                net.Conn
	tlsConn             *tls.Conn   // nil if plain TCP
	sendMu              sync.Mutex  // serialises writes to conn
	buf                 [16384]byte // per-miner send buffer; avoids per-write allocations
	onLogin             func(*Miner)
	onLoginReady        func(*Miner)
	onLoginEvent        func(*Miner)
	onSubmit            func(*Miner, *SubmitEvent)
	onClose             func(*Miner)
	closeOnce           sync.Once
}
