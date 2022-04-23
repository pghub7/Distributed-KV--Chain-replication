package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/kvslib"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
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
	clientNum := int8(arg)

	var config chainedkv.ClientConfig
	filePath := fmt.Sprintf("config/client_config_%d.json", clientNum)
	err = util.ReadJSONConfig(filePath, &config)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})

	client := kvslib.NewKVS()
	notifCh, err := client.Start(tracer, config.ClientID, config.CoordIPPort, config.LocalCoordIPPort, config.LocalHeadServerIPPort, config.LocalTailServerIPPort, config.ChCapacity)
	util.CheckErr(err, "Error reading client config: %v\n", err)
	//time.Sleep(4 * time.Second)
	// Put a key-value pair

	//for i := 1; i <= 20; i++ {
	//	key := strconv.Itoa(i)
	//	op, err := client.Put(tracer, config.ClientID, key, key)
	//	util.CheckErr(err, "Error putting value %v, opId: %v\b", err, op)
	//}

	// time.Sleep(1 * time.Second)
	//time.Sleep(4 * time.Second)

	for i := 0; i < 75; i++ {
		client.Put(tracer, config.ClientID, strconv.Itoa(i), "Value"+strconv.Itoa(i))
		client.Get(tracer, config.ClientID, strconv.Itoa(i))
	}

	//time.Sleep(5 * time.Second)

	for i := 0; i < 150; i++ {
		result := <-notifCh
		log.Println(result)
	}
	client.Stop()
}
