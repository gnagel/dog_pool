package dog_pool

import "net"

func findPort() (int, error) {
	addr, resolve_err := net.ResolveTCPAddr("tcp4", "127.0.0.1:0")
	if nil != resolve_err {
		return -1, resolve_err
	}

	listener, err := net.ListenTCP("tcp4", addr)
	switch {
	case nil != err:
		return -1, err
	default:
		addr := listener.Addr().(*net.TCPAddr)
		port := addr.Port
		listener.Close()
		return port, nil
	}
}
