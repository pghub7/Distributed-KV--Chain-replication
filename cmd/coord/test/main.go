package main

import (
	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"fmt"
	"net"
	"net/rpc"
	"os"
)

type ServerJoin struct {
	TailServer    chainedkv.ServerInfo
	ServerId      uint8
	JoinCompleted bool
	NumCalls      int
}

type ServerJoinAck struct {
	ServerId uint8
}

func (serverJoin *ServerJoin) FindTailServer(coordReply chainedkv.JoinResponse, reply *ServerJoinAck) error {
	serverJoin.NumCalls++
	serverJoin.TailServer = coordReply.TailServer
	*reply = ServerJoinAck{ServerId: coordReply.ServerId}
	fmt.Printf("ServerId: %v", coordReply.ServerId)
	fmt.Printf("Tail server: %v\n\n", serverJoin.TailServer)
	serverJoin.JoinCompleted = true
	return nil
}

func main() {

	laddr := "127.0.0.1:4322"
	coordAddr := "127.0.0.1:9876"
	coordListenAddress := "127.0.0.1:9875"

	coordListenAddr, err := net.ResolveTCPAddr("tcp", coordListenAddress)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	inboundCoord, err := net.ListenTCP("tcp", coordListenAddr)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	s := ServerJoin{ServerId: 1}
	err = rpc.Register(&s)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	go func() {
		for {
			rpc.Accept(inboundCoord)
		}
	}()

	serverLocalAddr, err := net.ResolveTCPAddr("tcp", laddr)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	coordRemoteAddr, err := net.ResolveTCPAddr("tcp", coordAddr)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	conn, err := net.DialTCP("tcp", serverLocalAddr, coordRemoteAddr)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	client := rpc.NewClient(conn)

	// serverList := []uint8{1,2,3}
	for i := 0; i < 5; i++ {
		go func(serverId int) {
			serverJoinRequest := chainedkv.ServerInfo{ServerId: uint8(serverId + 1), ServerAddr: laddr, CoordListenAddr: coordListenAddress}
			var reply bool
			err = client.Call("Coord.RequestServerJoin", serverJoinRequest, &reply)
			if err != nil {
				fmt.Println(err.Error())
			}
		}(i)
	}

	for {
		// if s.JoinCompleted {
		// 	fmt.Printf("Tail server: %v", s.TailServer)

		// }
		// if (s.NumCalls == 3) {
		// 	fmt.Println("all servers joined. Exiting")
		// 	break
		// }
	}
}
