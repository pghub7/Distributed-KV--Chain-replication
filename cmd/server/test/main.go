// delete later, for testing purposes so I know rpc is working...
package main

import (
	// "cs.ubc.ca/cpsc416/a3/chainedkv"
	// "cs.ubc.ca/cpsc416/a3/util"
	// "github.com/DistributedClocks/tracing"
	"cs.ubc.ca/cpsc416/a3/util"
	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"net"
	"net/rpc"
	"log"
	"fmt"
)
type Response struct {
	ServerListenAddr string
}


type Request struct {
	ServerId uint8
	ServerAddr string
	ServerListenAddr string
}

type ServerHandler struct {}

type EmptyRequest struct {
	ServerId uint8
}
type EmptyResponse struct {}

func (h *ServerHandler) Join(req Request, res *Response) (err error) {
	var config chainedkv.ServerConfig
	util.ReadJSONConfig("config/server_config_1.json", &config)
	/* if server_config_1, make it tail*/
	if req.ServerAddr == config.ServerAddr {
		res.ServerListenAddr = req.ServerListenAddr
		return
	}
	/* second server will know server from server_config is tail*/
	res.ServerListenAddr = config.ServerListenAddr
	return
}

func (h *ServerHandler) Joined(req EmptyRequest, res *EmptyResponse) (err error) {
	fmt.Printf("received 'Joined' from server %d\n", req.ServerId)
	return
}


func main(){
	// Publish our Handler methods
	var config chainedkv.CoordConfig
	util.ReadJSONConfig("config/coord_config.json", &config)
	handler := &ServerHandler{}
	rpc.Register(handler)

	// Create a TCP listener that will listen on `Port`
	tcpAddr, err := net.ResolveTCPAddr("tcp", config.ServerAPIListenAddr)
	if err != nil {
		log.Fatal(err)
	}

	// Close the listener whenever we stop
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal(err)
	}

	for {
		// Wait for incoming connections
		fmt.Println("waiting for incoming requests")
		rpc.Accept(listener)
	}
}