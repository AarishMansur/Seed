package client

import (
	"net"
	"net/http"
	"time"
)

func NewTunnedClient(maxCons int) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,

		MaxConnsPerHost:       maxCons,
		MaxIdleConnsPerHost:   maxCons,
		MaxIdleConns:          maxCons,
		DisableKeepAlives:     false,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   15 * time.Second,
	}

}
