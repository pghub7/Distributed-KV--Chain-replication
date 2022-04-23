package main

import (
	"log"
	"net/rpc"

	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/kvslib"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
)

// Structs

func main() {
	var config chainedkv.ClientConfig
	err := util.ReadJSONConfig("../../config/client_config.json", &config)
	util.CheckErr(err, "Error reading client config: %v\n", err)
	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})

	client := kvslib.NewKVS()
	err = rpc.Register(client)
	rpc.HandleHTTP()

	//listener, err := net.Listen("tcp", ":4040")
	rpcClient := rpc.NewClient(listener)
	//http.Serve(listener, nil)

	notifCh, err := client.Start(tracer, config.ClientID, config.CoordIPPort, config.LocalCoordIPPort, config.LocalHeadServerIPPort, config.LocalTailServerIPPort, config.ChCapacity)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	// Put a key-value pair
	op, err := client.Put(tracer, "clientID1", "key2", "value2")
	util.CheckErr(err, "Error putting value %v, opId: %v\b", err, op)

	// Get a key's value
	op, err = client.Get(tracer, "clientID1", "key1")
	util.CheckErr(err, "Error getting value %v, opId: %v\b", err, op)

	for i := 0; i < 2; i++ {
		result := <-notifCh
		log.Println(result)
	}
	client.Stop()
}
