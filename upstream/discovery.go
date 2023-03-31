package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/viper"
	"github.com/xjdrew/glog"
)

var _DB HostDB = HostDB{
	tables: map[string]*HostTable{},
}

type HostRecord struct {
	Name   string `json:"name"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
	Weight int    `json:"weight"`

	numVer uint64
	strVer string
	addr   string
}

type HostTable struct {
	weight  int
	numVer  uint64
	recDict map[string]*HostRecord // key: host addr
	recList []*HostRecord
}

func (p *HostTable) put(rec *HostRecord) {
	if _, ok := p.recDict[rec.addr]; !ok {
		p.weight += rec.Weight
		p.recDict[rec.addr] = rec
		p.recList = append(p.recList, rec)
	}
}

func (p *HostTable) delete(todel *HostRecord) bool {
	if rec, ok := p.recDict[todel.addr]; ok {
		p.weight -= rec.Weight
		delete(p.recDict, todel.addr)
		for i, rec := range p.recList {
			if rec.addr == todel.addr {
				p.recList = append(p.recList[:i], p.recList[i+1:]...)
				return true
			}
		}
	}
	return false
}

func (p *HostTable) size() int {
	return len(p.recList)
}

func (p *HostTable) query() string {
	// 停用按权重分配
	// w := rand.Intn(p.weight)
	// for _, rec := range p.records {
	// 	if rec.Weight >= w {
	// 		return rec.addr
	// 	}
	// 	w -= rec.Weight
	// }
	// return ""

	// 随机分配1个
	i := rand.Intn(len(p.recList))
	return p.recList[i].addr
}

type HostDB struct {
	mu     sync.RWMutex
	tables map[string]*HostTable // key: client version string
}

func (p *HostDB) put(rec *HostRecord) {
	defer p.mu.Unlock()
	p.mu.Lock()

	tb := p.tables[rec.strVer]
	if tb == nil {
		tb = &HostTable{
			numVer:  rec.numVer,
			recDict: map[string]*HostRecord{},
		}
		p.tables[rec.strVer] = tb
	}

	tb.put(rec)
}

func (p *HostDB) delete(todel *HostRecord) {
	defer p.mu.Unlock()
	p.mu.Lock()

	for strVer, tb := range p.tables {
		if tb.delete(todel) {
			if tb.size() == 0 {
				delete(p.tables, strVer)
			}
			break
		}
	}
}

func (p *HostDB) query(strVer string) string {
	defer p.mu.RUnlock()
	p.mu.RLock()

	if len(p.tables) == 0 {
		return ""
	}

	// 版本精确匹配
	if tb := p.tables[strVer]; tb != nil {
		return tb.query()
	}

	// 查找最临近的最小版本主机表
	numVer, err := toNumVer(strVer)
	if err != nil {
		glog.Errorf("invalid version from client: %v", strVer)
		glog.Flush()
		return ""
	}

	tables := []*HostTable{}
	for _, tb := range p.tables {
		tables = append(tables, tb)
	}

	sort.Slice(tables, func(i, j int) bool {
		lop := tables[i]
		rop := tables[j]
		return lop.numVer > rop.numVer
	})

	for _, tb := range tables {
		if numVer > tb.numVer {
			return tb.query()
		}
	}

	// 前端上传的版本号 比最小版本服还小
	return ""
}

func toNumVer(strVer string) (uint64, error) {
	strs := strings.Split(strVer, ".")
	if len(strs) != 3 {
		return 0, errors.New("invalid version string")
	}

	major, _ := strconv.Atoi(strs[0])
	minor, _ := strconv.Atoi(strs[1])
	revision, _ := strconv.Atoi(strs[2])

	// 版本号每个分量各16bit
	return (uint64(major) << 32) | (uint64(minor) << 16) | uint64(revision), nil
}

func parseHost(value []byte) (*HostRecord, error) {
	rec := &HostRecord{}
	err := json.Unmarshal(value, rec)
	if err != nil {
		return rec, err
	}

	// 拆分主机名
	fields := strings.Split(rec.Name, "_")
	if len(fields) != 3 {
		return nil, errors.New("client version not found from host name")
	}

	// 从主机名中提取版本号
	strVer := fields[2]
	numVer, err := toNumVer(strVer)
	if err != nil {
		return nil, errors.New("client version is invalid from host name")
	}

	rec.strVer = strVer
	rec.numVer = numVer
	rec.addr = fmt.Sprintf("%v:%v", rec.Host, rec.Port)
	return rec, nil
}

func openEtcd(etcdHost string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdHost},
		DialTimeout: 5 * time.Second,
	})
}

func watchEtcd(etcdHost, etcdPrefix string) {
	var cli *clientv3.Client
	var err error
	for {
		glog.Infof("begin connect etcd host %v", etcdHost)
		glog.Flush()
		cli, err = openEtcd(etcdHost)
		if err != nil {
			glog.Errorf("connect etcd host %v error: %v", etcdHost, err)
			glog.Flush()
			time.Sleep(time.Second)
			continue
		} else {
			glog.Infof("connect etcd host %v succeed", etcdHost)
			glog.Flush()
			break
		}
	}

	wch := cli.Watch(context.Background(), etcdPrefix, clientv3.WithPrefix())
	for msg := range wch {
		for _, ev := range msg.Events {
			rec, err := parseHost(ev.Kv.Value)
			if err != nil {
				glog.Errorf("etcd event decode error: %v, data: %v", err, string(ev.Kv.Value))
				glog.Flush()
				continue
			}

			switch ev.Type {
			case clientv3.EventTypePut:
				glog.Infof("PUT host %v:%v", rec.Host, rec.Port)
				glog.Flush()
				_DB.put(rec)

			case clientv3.EventTypeDelete:
				glog.Infof("DEL host %v:%v", rec.Host, rec.Port)
				glog.Flush()
				_DB.delete(rec)

			default:
				glog.Errorf("unexpected etcd event: %v", ev.Type)
				glog.Flush()
			}
		}
	}

	cli.Close()
	glog.Errorf("disconnect from etcd host: %v", etcdHost)
	glog.Flush()
}

// 模块api
func WatchHost() {
	etcdHost := viper.GetString("etcd_host")
	if etcdHost == "" {
		glog.Exit("etcd_host not found in config")
	}

	etcdPrefix := viper.GetString("etcd_prefix")
	if etcdPrefix == "" {
		glog.Exit("etcd_prefix not found in config")
	}

	go func() {
		for {
			watchEtcd(etcdHost, etcdPrefix)
		}
	}()

	glog.Infof("waiting for the first upstream host be online: %v", etcdHost)
	glog.Flush()
}

func QueryHost(strVer string) string {
	return _DB.query(strVer)
}
