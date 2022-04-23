package main

import (
	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/util"
	"fmt"
	"log"
	"github.com/DistributedClocks/tracing"
	"os"
	"strconv"
)

func main() {

	if len(os.Args) != 2 {
		fmt.Println("specify server config file number")
		return
	}
	arg, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal("Provided arg could not be converted to integer")
	}

	servNum := int8(arg)

	var config chainedkv.ServerConfig
	filePath := fmt.Sprintf("config/server_config_%d.json", servNum)
	util.ReadJSONConfig(filePath, &config)
	stracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	server := chainedkv.NewServer()
	server.Start(config.ServerId, config.CoordAddr, config.ServerAddr, config.ServerServerAddr, config.ServerListenAddr, config.ClientListenAddr, stracer)
}
