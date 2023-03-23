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

// 主机表静态实例
var _UL UList = UList{hosts: map[string]UHost{}}

// 主机记录
type UHost struct {
	Host   string `json:"host"`
	Port   string `json:"port"`
	Addr   string
	Weight int `json:"weight"`
}

// 主机表
type UList struct {
	mu          sync.RWMutex
	totalWeight int
	hosts       map[string]UHost
}

func (p *UList) put(uhost UHost) {
	defer p.mu.Unlock()
	p.mu.Lock()

	if _, ok := p.hosts[uhost.Addr]; !ok {
		p.totalWeight += uhost.Weight
		p.hosts[uhost.Addr] = uhost
	}
}

func (p *UList) delete(uhost UHost) {
	defer p.mu.Unlock()
	p.mu.Lock()

	if _, ok := p.hosts[uhost.Addr]; ok {
		p.totalWeight -= uhost.Weight
		delete(p.hosts, uhost.Addr)
	}
}

func (p *UList) roll() string {
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

func parseHost(value []byte) (UHost, error) {
	var uhost UHost
	err := json.Unmarshal(value, &uhost)
	if err != nil {
		return uhost, err
	}
	uhost.Addr = uhost.Host + ":" + uhost.Port
	return uhost, nil
}

func openEtcd(etcdHost string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdHost},
		DialTimeout: 5 * time.Second,
	})
}

func watchEtcd(etcdHost, etcdPrefix string, onFirstHost chan int) {
	cli, err := openEtcd(etcdHost)
	if err != nil {
		glog.Errorf("connect etcd host error: %v %v", etcdHost, err)
		time.Sleep(time.Second)
		return
	}

	wch := cli.Watch(context.Background(), etcdPrefix, clientv3.WithPrefix())
	for msg := range wch {
		for _, ev := range msg.Events {
			uhost, err := parseHost(ev.Kv.Value)
			if err != nil {
				glog.Errorf("unmarshal etcd event error: %v, data: %v", err, string(ev.Kv.Value))
				continue
			}
			switch ev.Type {
			case clientv3.EventTypePut:
				_UL.put(uhost)
				if onFirstHost != nil {
					onFirstHost <- 0
					onFirstHost = nil
				}
			case clientv3.EventTypeDelete:
				_UL.delete(uhost)
			default:
				glog.Errorf("unexpected etcd event: %v", ev.Type)
			}
		}
	}

	cli.Close()
	glog.Errorf("lose connection from etcd host: %v", etcdHost)
}

// 模块api
func WatchHost() {
	etcdHost := viper.GetString("etcd_host")
	if etcdHost == "" {
		panic("etcd_host not found in config")
	}

	etcdPrefix := viper.GetString("etcd_prefix")
	if etcdPrefix == "" {
		panic("etcd_prefix not found in config")
	}

	onFirstHost := make(chan int)
	go func() {
		for {
			watchEtcd(etcdHost, etcdPrefix, onFirstHost)
		}
	}()

	glog.Infof("waiting for the first upstream host be online: %v", etcdHost)
	glog.Flush()
	<-onFirstHost
}

func RollHost() string {
	return _UL.roll()
}
