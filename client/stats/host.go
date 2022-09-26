package stats

import (
	"math"
	"runtime"
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
)

// HostStats represents resource usage stats of the host running a Jobpool client
type HostStats struct {
	Memory           *MemoryStats
	DiskStats        []*DiskStats
	AllocDirStats    *DiskStats
	Uptime           uint64
	Timestamp        int64
	CPUTicksConsumed float64
}

// MemoryStats represents stats related to virtual memory usage
type MemoryStats struct {
	Total     uint64
	Available uint64
	Used      uint64
	Free      uint64
}

// DiskStats represents stats related to disk usage
type DiskStats struct {
	Device            string
	Mountpoint        string
	Size              uint64
	Used              uint64
	Available         uint64
	UsedPercent       float64
	InodesUsedPercent float64
}

// NodeStatsCollector is an interface which is used for the purposes of mocking
// the HostStatsCollector in the tests
type NodeStatsCollector interface {
	Collect() error
	Stats() *HostStats
}

// HostStatsCollector collects host resource usage stats
type HostStatsCollector struct {
	numCores             int
	hostStats            *HostStats
	hostStatsLock        sync.RWMutex
	allocDir             string

	// badParts is a set of partitions whose usage cannot be read; used to
	// squelch logspam.
	badParts map[string]struct{}

	logger hclog.Logger
}

// NewHostStatsCollector returns a HostStatsCollector. The allocDir is passed in
// so that we can present the disk related statistics for the mountpoint where
// the allocation directory lives
func NewHostStatsCollector(logger hclog.Logger, allocDir string) *HostStatsCollector {
	logger = logger.Named("host_stats")
	numCores := runtime.NumCPU()
	collector := &HostStatsCollector{
		numCores:             numCores,
		logger:               logger,
		allocDir:             allocDir,
		badParts:             make(map[string]struct{}),
	}
	return collector
}

// Collect collects stats related to resource usage of a host
func (h *HostStatsCollector) Collect() error {
	h.hostStatsLock.Lock()
	defer h.hostStatsLock.Unlock()
	return h.collectLocked()
}

// collectLocked collects stats related to resource usage of the host but should
// be called with the lock held.
func (h *HostStatsCollector) collectLocked() error {
	hs := &HostStats{Timestamp: time.Now().UTC().UnixNano()}

	// Determine up-time
	uptime, err := host.Uptime()
	if err != nil {
		h.logger.Error("failed to collect upstime stats", "error", err)
		uptime = 0
	}
	hs.Uptime = uptime

	// Collect memory stats
	mstats, err := h.collectMemoryStats()
	if err != nil {
		h.logger.Error("failed to collect memory stats", "error", err)
		mstats = &MemoryStats{}
	}
	hs.Memory = mstats
	// Collect disk stats
	diskStats, err := h.collectDiskStats()
	if err != nil {
		h.logger.Error("failed to collect disk stats", "error", err)
		hs.DiskStats = []*DiskStats{}
	}
	hs.DiskStats = diskStats

	// Getting the disk stats for the allocation directory
	usage, err := disk.Usage(h.allocDir)
	if err != nil {
		h.logger.Error("failed to find disk usage of alloc", "alloc_dir", h.allocDir, "error", err)
		hs.AllocDirStats = &DiskStats{}
	} else {
		hs.AllocDirStats = h.toDiskStats(usage, nil)
	}
	// Update the collected status object.
	h.hostStats = hs

	return nil
}

func (h *HostStatsCollector) collectMemoryStats() (*MemoryStats, error) {
	memStats, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}
	mem := &MemoryStats{
		Total:     memStats.Total,
		Available: memStats.Available,
		Used:      memStats.Used,
		Free:      memStats.Free,
	}

	return mem, nil
}

func (h *HostStatsCollector) collectDiskStats() ([]*DiskStats, error) {
	partitions, err := disk.Partitions(false)
	if err != nil {
		return nil, err
	}

	var diskStats []*DiskStats
	for _, partition := range partitions {
		usage, err := disk.Usage(partition.Mountpoint)
		if err != nil {
			if _, ok := h.badParts[partition.Mountpoint]; ok {
				// already known bad, don't log again
				continue
			}

			h.badParts[partition.Mountpoint] = struct{}{}
			h.logger.Warn("error fetching host disk usage stats", "error", err, "partition", partition.Mountpoint)
			continue
		}
		delete(h.badParts, partition.Mountpoint)

		ds := h.toDiskStats(usage, &partition)
		diskStats = append(diskStats, ds)
	}

	return diskStats, nil
}

// Stats returns the host stats that has been collected
func (h *HostStatsCollector) Stats() *HostStats {
	h.hostStatsLock.RLock()
	defer h.hostStatsLock.RUnlock()

	if h.hostStats == nil {
		if err := h.collectLocked(); err != nil {
			h.logger.Warn("error fetching host resource usage stats", "error", err)
		}
	}

	return h.hostStats
}

// toDiskStats merges UsageStat and PartitionStat to create a DiskStat
func (h *HostStatsCollector) toDiskStats(usage *disk.UsageStat, partitionStat *disk.PartitionStat) *DiskStats {
	ds := DiskStats{
		Size:              usage.Total,
		Used:              usage.Used,
		Available:         usage.Free,
		UsedPercent:       usage.UsedPercent,
		InodesUsedPercent: usage.InodesUsedPercent,
	}
	if math.IsNaN(ds.UsedPercent) {
		ds.UsedPercent = 0.0
	}
	if math.IsNaN(ds.InodesUsedPercent) {
		ds.InodesUsedPercent = 0.0
	}

	if partitionStat != nil {
		ds.Device = partitionStat.Device
		ds.Mountpoint = partitionStat.Mountpoint
	}

	return &ds
}