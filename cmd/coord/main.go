package main

import (
	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/util"
	"fmt"
	"github.com/DistributedClocks/tracing"
	// "cs.ubc.ca/cpsc416/a3/fcheck"
)

func main() {
	var config chainedkv.CoordConfig
	util.ReadJSONConfig("config/coord_config.json", &config)
	ctracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	defer ctracer.Close()
	ctrace := ctracer.CreateTrace()

	coord := chainedkv.NewCoord()
	coord.Trace = ctrace

	err := coord.Start(config.ClientAPIListenAddr, config.ServerAPIListenAddr, config.LostMsgsThresh, config.NumServers, ctracer)
	if err != nil {
		fmt.Println("Failure in coord.Start: ", err.Error())
	}

}
