package proxy

// WorkerRow is one row in the /1/workers table.
//
//	WorkerRow{"rig-alpha", "10.0.0.1", 1, 10, 0, 0, 100000, 1712232000, 1.0, 1.0, 1.0, 1.0, 1.0}
type WorkerRow [13]any

// MinerRow is one row in the /1/miners table.
//
//	MinerRow{1, "10.0.0.1:49152", 4096, 512, 2, 100000, "WALLET", "********", "rig-alpha", "XMRig/6.21.0"}
type MinerRow [10]any
