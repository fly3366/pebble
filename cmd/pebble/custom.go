package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/cmd/pebble/gorocksdb"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/dgraph-io/badger/v2"
	"github.com/dustin/go-humanize"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/v3/disk"
)

func metric() (float64, float64, float64, float64, int64, float64) {
	percent, err := cpu.Percent(time.Second, false)
	if err != nil {
		log.Default().Printf("GET CPU Usage failed: %v\n", err)
	}

	mem, err := mem.VirtualMemory()
	if err != nil {
		log.Default().Printf("GET MEM Usage failed: %v\n", err)
	}

	log.Printf("CPU INFO: %+v \n", percent[0])
	log.Printf("MEM INFO: %+v \n", mem.UsedPercent)

	ioStat, _ := disk.IOCounters()
	var ioutilsMetric float64
	var ioTotal float64
	var ioTime float64

	rTime := time.Now().UnixNano()

	for k, v := range ioStat {
		devStr := os.Getenv("DEVICE")
		if !strings.Contains(k, devStr) {
			continue
		}

		ioTotal = float64(v.ReadCount + v.WriteCount)
		ioTime = float64(v.IoTime)

		ioutilsMetric = float64(v.WriteTime + v.ReadTime)
	}

	return percent[0], mem.UsedPercent, ioutilsMetric, ioTotal, rTime, ioTime
}

var pbCache *pebble.Cache
var rkCache *gorocksdb.Cache

func getDbInstance(dir string) (DB, error) {

	engine := os.Getenv("ENGINE")
	// MB
	cacheSizeStr := os.Getenv("CACHE_SIZE")
	cacheTypeStr := os.Getenv("CACHE_TYPE")

	// cache
	cacheSize32, err := strconv.Atoi(cacheSizeStr)
	if err != nil {
		panic("parse err")
	}

	switch engine {
	case "pebble":
		var cache *pebble.Cache
		if cacheTypeStr == "shared" {
			if pbCache == nil {
				pbCache = pebble.NewCache(int64(cacheSize32) * humanize.MiByte)
			}

			cache = pbCache
		} else {
			cache = pebble.NewCache(int64(cacheSize32) * humanize.MiByte)
		}

		opts := &pebble.Options{
			Cache:                       cache,        // block cache
			Comparer:                    mvccComparer, // key comparer
			DisableWAL:                  false,        // close wal, may ture when on disk sort
			FormatMajorVersion:          pebble.FormatNewest,
			L0CompactionThreshold:       2,                              // 多少L0读放大时开始L0压缩
			L0StopWritesThreshold:       1000,                           // 多少L0读放大时停写，一定大于上面的设置，否则永远不会压缩
			LBaseMaxBytes:               64 << 20,                       // L1 大小
			Levels:                      make([]pebble.LevelOptions, 7), // 每= level 详细设置
			MaxConcurrentCompactions:    2,                              // 最大压缩并发
			MaxOpenFiles:                16384,                          // 最大fd
			MemTableSize:                64 << 20,                       // max memtable 大小，默认从 256K 开始增长
			MemTableStopWritesThreshold: 4,                              // 最大 memtable flush 队列，超过后会停止写
			Merger: &pebble.Merger{
				Name: "cockroach_merge_operator",
			},
			// FlushSplitBytes:             0,                    // L0 SubLevel 范围
			// BytesPerSync:        64 * humanize.KiByte, // 写多少后主动 sync
			Cleaner: pebble.DeleteCleaner{},
			// MaxManifestFileSize: 64 * humanize.KiByte, // manifest 文件大小
			// WALDir:              "/dev/shm/test/wal",  // wal save path
		}

		for i := 0; i < len(opts.Levels); i++ {
			l := &opts.Levels[i]
			l.BlockSize = 32 << 10       // 32 KB
			l.IndexBlockSize = 256 << 10 // 256 KB
			l.FilterPolicy = bloom.FilterPolicy(10)
			l.FilterType = pebble.TableFilter
			if i > 0 {
				l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
			}
			l.EnsureDefaults()
		}
		opts.Levels[6].FilterPolicy = nil
		// flush 分段使用 L0 文件大小
		opts.FlushSplitBytes = opts.Levels[0].TargetFileSize
		opts.EnsureDefaults()

		if verbose {
			opts.EventListener = pebble.MakeLoggingEventListener(nil)
			opts.EventListener.TableDeleted = nil
			opts.EventListener.TableIngested = nil
			opts.EventListener.WALCreated = nil
			opts.EventListener.WALDeleted = nil
		}

		db, err := pebble.Open(dir, opts)
		if err != nil {
			return nil, err
		}

		return pebbleDB{
			d:       db,
			ballast: make([]byte, 1<<30),
		}, nil
	case "rocks":
		var cache *gorocksdb.Cache
		if cacheTypeStr == "shared" {
			if rkCache == nil {
				rkCache = gorocksdb.NewLRUCache(cacheSize32 * humanize.MiByte)
			}

			cache = rkCache
		} else {
			cache = gorocksdb.NewLRUCache(cacheSize32 * humanize.MiByte)
		}

		opts := gorocksdb.NewDefaultOptions()
		wo := gorocksdb.NewDefaultWriteOptions()
		ro := gorocksdb.NewDefaultReadOptions()
		bbto := gorocksdb.NewDefaultBlockBasedTableOptions()

		opts.SetArenaBlockSize(64 << 20)
		opts.SetMaxBackgroundCompactions(2)
		opts.SetMaxOpenFiles(16384)

		opts.SetCreateIfMissing(true)
		opts.SetUseFsync(true)

		bbto.SetBlockCache(cache)

		db, err := gorocksdb.OpenDb(opts, dir)
		if err != nil {
			log.Fatal(err)
		}

		return &rocksDB{
			db:    db,
			bbto:  bbto,
			ro:    ro,
			wo:    wo,
			cache: cache,
		}, nil
	case "badger":
		if cacheTypeStr == "shared" {
			panic("Unsupport!")
		}

		opts := badger.DefaultOptions(dir)
		opts = opts.WithBlockCacheSize(int64(cacheSize32) * humanize.MiByte)
		opts = opts.WithBlockSize(32 * humanize.KiByte)
		opts = opts.WithLoadBloomsOnOpen(true)
		opts = opts.WithSyncWrites(true)

		db, err := badger.Open(opts)
		if err != nil {
			log.Fatal(err)
		}
		return &badgerDB{db}, nil
	default:
		panic("Unkown Engine")
	}
}

func main() {
	log.SetFlags(0)
	wipe = true
	verbose = false

	closeCh := make(chan struct{})

	conStr := os.Getenv("CON")
	instanceStr := os.Getenv("INSNUM")
	dir := os.Getenv("PATH")

	// 总并发
	var err error
	concurrency, err = strconv.Atoi(conStr)
	if err != nil {
		panic("parse err")
	}
	// instance 数
	instanceNum, err := strconv.Atoi(instanceStr)
	if err != nil {
		panic("parse err")
	}

	duration = time.Minute

	if wipe {
		fmt.Printf("wiping %s\n", dir)
		if err := os.RemoveAll(dir); err != nil {
			log.Fatal(err)
		}
	}

	os.Mkdir(dir, 0777)

	fmt.Printf("dir %s\nconcurrency %d\n", dir, concurrency)

	println("Start Test")

	dbs := make([]DB, 0)

	for i := 0; i < instanceNum; i++ {
		db, err := getDbInstance(dir + strconv.Itoa(i))
		if err != nil {
			log.Fatalf("cannot open: %v\n", err)
			os.Exit(1)
		}
		dbs = append(dbs, db)
	}

	cpuAvg := make([]float64, 0)
	ioAvg := make([]float64, 0)
	ioTotal := make([]float64, 0)
	ioTimeSeq := make([]float64, 0)
	rTimeSeq := make([]float64, 0)

	go func() {
		t := time.NewTimer(10 * time.Second)
		for {
			select {
			case <-closeCh:
				return
			case <-t.C:
				cpu, _, io, iot, rTime, ioTime := metric()

				cpuAvg = append(cpuAvg, cpu)
				ioAvg = append(ioAvg, io)
				ioTotal = append(ioTotal, iot)
				rTimeSeq = append(rTimeSeq, float64(rTime))
				ioTimeSeq = append(ioTimeSeq, ioTime)

				t.Reset(5 * time.Second)
			}
		}
	}()

	vSizeStr := os.Getenv("VALUE_SIZE")
	bSizeStr := os.Getenv("BATCH_SIZE")
	durationStr := os.Getenv("DURATION")
	durationNum, err := strconv.Atoi(durationStr)
	if err != nil {
		err = nil
		durationNum = 60
	}

	duration = time.Duration(durationNum) * time.Second

	// ycsb
	ycsbConfig.batch = randvar.NewFlag(bSizeStr)
	ycsbConfig.keys = "zipf"
	ycsbConfig.initialKeys = 10000
	ycsbConfig.prepopulatedKeys = 0
	ycsbConfig.numOps = 0
	ycsbConfig.scans = randvar.NewFlag("zipf:1-1000")
	ycsbConfig.workload = "A"
	// 1k
	ycsbConfig.values = randvar.NewBytesFlag(vSizeStr)

	var wg sync.WaitGroup
	for i := 0; i < instanceNum; i++ {
		wg.Add(1)
		db := dbs[i]

		go func() {
			defer wg.Done()

			RunYcsbRuntime(db)
		}()
	}

	// err = db.Close()
	// if err != nil {
	// 	log.Fatalf("cannot close db: %v\n", err)
	// }

	wg.Wait()

	closeCh <- struct{}{}
	close(closeCh)

	awaitStream := make([]float64, 0)
	utilStream := make([]float64, 0)
	ioTimeStream := make([]float64, 0)

	var totalCPU float64

	var maxCPU float64
	var maxIO float64

	var avgIOTime float64

	for k, v := range cpuAvg {
		totalCPU += v

		if maxCPU < v {
			maxCPU = v
		}

		if k != 0 {
			avgIOTime += (ioTimeSeq[k] - ioTimeSeq[k-1]) / (ioTotal[k] - ioTotal[k-1])

			if maxIO < (ioTimeSeq[k]-ioTimeSeq[k-1])/(ioTotal[k]-ioTotal[k-1]) {
				maxIO = (ioTimeSeq[k] - ioTimeSeq[k-1]) / (ioTotal[k] - ioTotal[k-1])
			}
			// diff
			// fmt.Printf("IO seq: %v %v %v \n", ioAvg[k]-ioAvg[k-1], ioTotal[k]-ioTotal[k-1], ioAvg[k]-ioAvg[k-1]/ioTotal[k]-ioTotal[k-1])

			if ioTotal[k]-ioTotal[k-1] <= 0 {
				awaitStream = append(awaitStream, 0)
			} else {
				awaitStream = append(awaitStream, (ioAvg[k]-ioAvg[k-1])/(ioTimeSeq[k]-ioTimeSeq[k-1]))
			}

			util := float64(ioTimeSeq[k]-ioTimeSeq[k-1]) / ((rTimeSeq[k] - rTimeSeq[k-1]) / float64(time.Millisecond))

			if util > 1 {
				utilStream = append(utilStream, 1)
			} else {
				utilStream = append(utilStream, util)
			}

			ioTimeStream = append(ioTimeStream, (ioTimeSeq[k]-ioTimeSeq[k-1])/(ioTotal[k]-ioTotal[k-1]))
		}
	}

	fmt.Printf("------ report -------\n")
	fmt.Printf("-------------\n")
	fmt.Printf("CPU avg: %v \n", totalCPU/float64(len(cpuAvg)))
	fmt.Printf("CPU max: %v \n", maxCPU)
	fmt.Printf("-------------\n")

	fmt.Printf("IOtime avg: %v \n", avgIOTime/float64(len(cpuAvg)-1))
	fmt.Printf("IOtime max: %v \n", maxIO)

	fmt.Printf("-------------\n")

	fmt.Printf("CPU seq: %v \n", cpuAvg)
	fmt.Printf("IOtime seq(ms): %v \n", ioTimeStream)
	fmt.Printf("await seq: %v \n", awaitStream)
	fmt.Printf("util seq(%%): %v \n", utilStream)

	println("End Test")
}

// func p(a uint64) ([]byte, []byte) {
// 	b1 := make([]byte, 9)
// 	b2 := make([]byte, 9)

// 	b1[0] = byte(0x10)
// 	b2[0] = byte(0x10)

// 	binary.LittleEndian.PutUint64(b1[1:], a)
// 	binary.BigEndian.PutUint64(b2[1:], a)

// 	return b1, b2
// }

// func main() {
// 	opts := &pebble.Options{
// 		Cache:                       nil,          // block cache
// 		Comparer:                    mvccComparer, // key comparer
// 		DisableWAL:                  false,        // close wal, may ture when on disk sort
// 		FormatMajorVersion:          pebble.FormatNewest,
// 		L0CompactionThreshold:       2,                              // 多少L0读放大时开始L0压缩
// 		L0StopWritesThreshold:       1000,                           // 多少L0读放大时停写，一定大于上面的设置，否则永远不会压缩
// 		LBaseMaxBytes:               64 << 20,                       // L1 大小
// 		Levels:                      make([]pebble.LevelOptions, 7), // 每= level 详细设置
// 		MaxConcurrentCompactions:    2,                              // 最大压缩并发
// 		MaxOpenFiles:                16384,                          // 最大fd
// 		MemTableSize:                64 << 20,                       // max memtable 大小，默认从 256K 开始增长
// 		MemTableStopWritesThreshold: 4,                              // 最大 memtable flush 队列，超过后会停止写
// 		Merger: &pebble.Merger{
// 			Name: "cockroach_merge_operator",
// 		},
// 		// FlushSplitBytes:             0,                    // L0 SubLevel 范围
// 		// BytesPerSync:        64 * humanize.KiByte, // 写多少后主动 sync
// 		Cleaner: pebble.DeleteCleaner{},
// 		// MaxManifestFileSize: 64 * humanize.KiByte, // manifest 文件大小
// 		// WALDir:              "/dev/shm/test/wal",  // wal save path
// 	}

// 	for i := 0; i < len(opts.Levels); i++ {
// 		l := &opts.Levels[i]
// 		l.BlockSize = 32 << 10       // 32 KB
// 		l.IndexBlockSize = 256 << 10 // 256 KB
// 		l.FilterPolicy = bloom.FilterPolicy(10)
// 		l.FilterType = pebble.TableFilter
// 		if i > 0 {
// 			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
// 		}
// 		l.EnsureDefaults()
// 	}
// 	opts.Levels[6].FilterPolicy = nil
// 	// flush 分段使用 L0 文件大小
// 	opts.FlushSplitBytes = opts.Levels[0].TargetFileSize
// 	opts.EnsureDefaults()

// 	if verbose {
// 		opts.EventListener = pebble.MakeLoggingEventListener(nil)
// 		opts.EventListener.TableDeleted = nil
// 		opts.EventListener.TableIngested = nil
// 		opts.EventListener.WALCreated = nil
// 		opts.EventListener.WALDeleted = nil
// 	}

// 	fmt.Printf("wiping %s\n", "/dev/shm/pb/test")
// 	if err := os.RemoveAll("/dev/shm/pb/test"); err != nil {
// 		log.Fatal(err)
// 	}

// 	db, err := pebble.Open("/dev/shm/pb/test", opts)
// 	if err != nil {
// 		panic(err)
// 	}

// 	var wg sync.WaitGroup
// 	for i := uint64(0); i < 10; i++ {
// 		wg.Add(1)
// 		go func(i uint64) {
// 			nb := db.NewBatch()
// 			for j := uint64(i * 102400); j < (i+1)*102400; j++ {
// 				le0, _ := p(j)
// 				nb.Set(le0, []byte{0}, &pebble.WriteOptions{
// 					Sync: true,
// 				})
// 			}
// 			nb.Commit(pebble.Sync)
// 		}(i)
// 	}
// 	wg.Wait()

// 	iter := db.NewIter(&pebble.IterOptions{})

// 	for iter.SeekGE([]byte{0x10}); ; iter.Next() {
// 		ok := iter.Valid()
// 		if !ok {
// 			break
// 		}
// 		key := iter.Key()
// 		if bytes.Compare(key, []byte{0x10, 0xff, 0xff, 0xff}) >= 0 {
// 			break
// 		}

// 		fmt.Printf("key: %v - rawKey: %v\n", binary.LittleEndian.Uint64(key[1:]), key)
// 	}

// 	db.Close()
// }
