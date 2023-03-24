package upstream

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/viper"
	"github.com/xjdrew/glog"
)

var _DB HostDB = HostDB{hosts: map[string]HostRecord{}}

type HostRecord struct {
	Host   string `json:"host"`
	Port   string `json:"port"`
	Addr   string
	Weight int `json:"weight"`
}

type HostDB struct {
	mu          sync.RWMutex
	totalWeight int
	hosts       map[string]HostRecord
}

func (p *HostDB) put(rec HostRecord) {
	defer p.mu.Unlock()
	p.mu.Lock()

	if _, ok := p.hosts[rec.Addr]; !ok {
		p.totalWeight += rec.Weight
		p.hosts[rec.Addr] = rec
	}
}

func (p *HostDB) delete(rec HostRecord) {
	defer p.mu.Unlock()
	p.mu.Lock()

	if _rec, ok := p.hosts[rec.Addr]; ok {
		p.totalWeight -= _rec.Weight
		delete(p.hosts, _rec.Addr)
	}
}

func (p *HostDB) roll() string {
	defer p.mu.RUnlock()
	p.mu.RLock()
	w := rand.Intn(p.totalWeight)
	for _, host := range p.hosts {
		if host.Weight >= w {
			return host.Addr
		}
		w -= host.Weight
	}
	return ""
}

func parseHost(value []byte) (HostRecord, error) {
	var rec HostRecord
	err := json.Unmarshal(value, &rec)
	if err != nil {
		return rec, err
	}
	rec.Addr = rec.Host + ":" + rec.Port
	return rec, nil
}

func openEtcd(etcdHost string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdHost},
		DialTimeout: 5 * time.Second,
	})
}

func watchEtcd(etcdHost, etcdPrefix string, onFirstHost chan int) {
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
				glog.Errorf("unmarshal etcd event error: %v, data: %v", err, string(ev.Kv.Value))
				glog.Flush()
				continue
			}
			switch ev.Type {
			case clientv3.EventTypePut:
				glog.Infof("PUT host %v:%v", rec.Host, rec.Port)
				glog.Flush()
				_DB.put(rec)
				if onFirstHost != nil {
					onFirstHost <- 0
					onFirstHost = nil
				}
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

	onFirstHost := make(chan int)
	go func() {
		watchEtcd(etcdHost, etcdPrefix, onFirstHost)
		for {
			watchEtcd(etcdHost, etcdPrefix, nil)
		}
	}()

	glog.Infof("waiting for the first upstream host be online: %v", etcdHost)
	glog.Flush()
	<-onFirstHost
}

func RollHost() string {
	return _DB.roll()
}
