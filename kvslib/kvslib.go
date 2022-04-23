package kvslib

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/DistributedClocks/tracing"
)

// Actions to be recorded by kvslib (as part of ktrace, put trace, get trace):

type KvslibStart struct {
	ClientId string
	Token    tracing.TracingToken
}

type KvslibStop struct {
	ClientId string
}

type HeadTailServerRequest struct {
	ClientId string
	Type     string
	Token    tracing.TracingToken
}

type HeadTailServerReply struct {
	HeadServerAddress string
	TailServerAddress string
	ServerId          uint8
	Token             tracing.TracingToken
}

type Put struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type PutRequest struct {
	ClientId              string
	OpId                  uint32
	Key                   string
	Value                 string
	LocalTailServerIPPort string
	Token                 tracing.TracingToken
}

type PutResponse struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type PutResultRecvd struct {
	OpId uint32
	GId  uint64
	Key  string
}

type Get struct {
	ClientId string
	OpId     uint32
	Key      string
	Token    tracing.TracingToken
}

type GetResponse struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type GetResultRecvd struct {
	OpId  uint32
	GId   uint64
	Key   string
	Value string
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId   uint32
	GId    uint64
	Result string
}

type KVS struct {
	notifyCh NotifyChannel
	stopChan chan bool
	// Add more KVS instance state here.
	putReceivedChan        chan PutResult
	clientID               string
	localHeadServerIPPort  string
	localTailServerIPPort  string
	remoteHeadServerIPPort string
	remoteTailServerIPPort string
	coordTCPConn           *net.TCPConn
	coordRpcClient         *rpc.Client
	headServerRpcClient    *rpc.Client
	tailServerRpcClient    *rpc.Client
	localTailListener      net.Listener
	kvsTracer              *tracing.Tracer
	currOpID               uint32
	currSentOpID           uint32
	mu                     sync.Mutex
	muTwo                  sync.Mutex
	changeHeadChan         chan bool
	changeTailChan         chan bool
	localCoordListAddr     string
}

type PutResult struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type HeadReq struct {
	ClientId string
}

type HeadResRecvd struct {
	ClientId string
	ServerId uint8
}

type TailReq struct {
	ClientId string
}

type TailResRecvd struct {
	ClientId string
	ServerId uint8
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh: nil,
	}
}

//var globalClientInstances map[string]*KVS = make(map[string]*KVS)
var ongoingPut sync.Map
var alreadyRegistered bool = false

var changeHeadAddress bool = false
var changeTailAddress bool = false

var counter int = 0

type CoordNotification int

// Start Starts the instance of KVS to use for connecting to the system with the given coord's IP:port.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the coord, this should return an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, coordIPPort string, localCoordIPPort string, localHeadServerIPPort string, localTailServerIPPort string, chCapacity int) (NotifyChannel, error) {
	//Initialize member variables
	var err error
	d.kvsTracer = localTracer
	// Channel that has a value if the head is being rerequested by a client
	d.changeHeadChan = make(chan bool, 1)
	// Channel that has a value if the tail is being rerequested by a client
	d.changeTailChan = make(chan bool, 1)
	// Channel that receives a PutResult object from tailserver using d.localTailListener
	d.putReceivedChan = make(chan PutResult, 1)
	// Channel that halts everything with a value, when STOP() is called
	d.stopChan = make(chan bool, 1)
	d.localCoordListAddr = localCoordIPPort

	var coordNotify CoordNotification
	go startListeningForCoord(d.localCoordListAddr, &coordNotify)

	kvsTracer := d.kvsTracer.CreateTrace()
	kvsTracer.RecordAction(KvslibStart{
		ClientId: clientId,
	})
	d.notifyCh = make(chan ResultStruct, chCapacity)
	d.clientID = clientId

	// Each client has a different port to listen to connect to Coord, getting rid of the bind already used error

	openPort, err := getFreePort("127.0.0.1")
	for {
		if err != nil {
			openPort, err = getFreePort("127.0.0.1")
		} else {
			break
		}

	}
	laddr, _ := net.ResolveTCPAddr("tcp", openPort)
	raddr, _ := net.ResolveTCPAddr("tcp", coordIPPort)
	coordConn, err := net.DialTCP("tcp", laddr, raddr)
	d.coordTCPConn = coordConn
	if err != nil {
		print(err.Error())
		connectError := errors.New("Unable To connect to Coord Node")
		return nil, connectError
	}
	d.coordRpcClient = rpc.NewClient(coordConn)

	d.localHeadServerIPPort = localHeadServerIPPort
	d.localTailServerIPPort = localTailServerIPPort

	// Sets upt the localtaillistener that will listen for rpc calls from tailserver PUT responses
	go func() {
		localTailListener, err := net.Listen("tcp", d.localTailServerIPPort)
		//defer localTailListener.Close()
		if err != nil {
			//print("VALORUS\n\n\n")
		}
		//defer localTailListener.Close()

		for {
			conn, err := localTailListener.Accept()
			if err != nil {
				//print("NO\n\n")
				fmt.Printf("rpc.Serve: accept: %s", err)
			}
			go rpc.ServeConn(conn)
		}
	}()

	finishChan := make(chan bool, 1)
	d.changeHeadChan <- true
	d.updateServerConnection("head", finishChan)
two:
	for {
		select {
		case <-finishChan:

			break two
		default:

		}

	}

	d.changeTailChan <- true
	d.updateServerConnection("tail", finishChan)
one:
	for {
		select {
		case <-finishChan:

			break one
		default:

		}

	}

	d.currOpID = 0
	d.currSentOpID = 1
	if !alreadyRegistered {
		rpc.Register(d)
	}
	//globalClientInstances[clientId] = d

	return d.notifyCh, nil
}

// Get  non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The returned value must be delivered asynchronously to the client via the notify-channel channel returned in the Start call.
// The value opId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	// TODO: find a way to make sure this is non-blocking, DISTRIBUTED TRACING
	//d.tailServerRpcClient
	for {
		if len(d.changeTailChan) > 0 {
			continue
		} else {
			break
		}

	}
	getTracer := tracer.CreateTrace() // order
	// client.go calls the rpc function asynchrnonously
	//d.coordRpcClient.Go("ClientLearnServers.GetHeadTailServer", tailServerRequest, &headTailReply, getHeadServerChan)
	errorChan := make(chan error, 1)
	var err error
	err = nil
	var currID uint32
	currID = d.IncOPId()
	if clientId != d.clientID {
		return 0, errors.New("Not the same clientID")
	}

	//var setServerChannel chan bool
	go func() {
	out:
		select {
		case <-d.stopChan:
			break out
		// The call for the tailServer finishes
		default:
			//finishedChan := make(chan *rpc.Call, 1)
			//getFinishedChan := make(chan *rpc.Call, 1)
			//loop for executing and receiving GET request/response
			for {
				if d.currSentOpID == currID {
					//print("EMFT\n\n\n\n")
					getMessage := Get{
						ClientId: clientId,
						OpId:     currID,
						Key:      key,
						Token:    nil,
					}

					getTracer.RecordAction(getMessage)
					getMessage.Token = getTracer.GenerateToken()
					var getResponse GetResponse

					// Buffer if existing put for key is being done, also buffers according to opid
					for {
						_, exists := ongoingPut.Load(key)
						if !exists {
							break
						}
					}

					// Loop for GET, keep passing it until call succeeds
					//var call *rpc.Call

					for {
						call := d.tailServerRpcClient.Go("Server.Get", getMessage, &getResponse, nil)
					three:
						for {
							select {
							case <-call.Done:

								break three
							default:

							}

						}
						//print("CALLAGAIN\n\n")
						if call.Error != nil {
							//print("CALL\n\n")
							var finishChan chan bool
						off:
							for {
								if len(d.changeTailChan) == 0 {

									finishChan = make(chan bool, 1)
									d.changeTailChan <- true
									d.updateServerConnection("tail", finishChan)

									//off:
									for {
										select {
										case <-finishChan:
											break off
										default:

										}

									}
								}
							}

						} else {
							//newTailServer:
							for {
								select {
								case <-d.stopChan:
									break out
								default:
									/*if call.Error != nil {
										errorChan <- errors.New("Unable to connect to tail Server50")
										break newTailServer
									}*/
									// Else record trace of GET Response

									receivedTrace := tracer.ReceiveToken(getResponse.Token)
									receivedTrace.RecordAction(GetResultRecvd{
										OpId:  getResponse.OpId,
										GId:   getResponse.GId,
										Key:   getResponse.Key,
										Value: getResponse.Value,
									})

									// Add the resultStruct to the channel
									// If the get is on something that has not been put before, getResultRecv.Value should be nil

									var getResultStruct ResultStruct
									getResultStruct = ResultStruct{
										OpId:   getResponse.OpId,
										GId:    getResponse.GId,
										Result: getResponse.Value,
									}

									d.notifyCh <- getResultStruct
									d.mu.Lock()
									d.currSentOpID = d.currSentOpID + 1
									d.mu.Unlock()
									break out
									// Record current and Increment OP_ID

								}

							}
						}

					}
				}

			}
		}

	}()
	select {
	case getError := <-errorChan:
		err = getError
		print(err.Error())
	default:

	}
	return currID, err

}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value opId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	// reinitialize the KVS struct member channel that notifies when a put result is received back to client
	if clientId != d.clientID {
		return 0, errors.New("Not the same clientID")
	}

	for {
		//print("1\n\n\n")
		if len(d.changeHeadChan) > 0 {
			continue
		} else {
			break
		}

	}
	d.putReceivedChan = make(chan PutResult, 1)
	//Make call to Coord to get headServerIPPort
	// Call it asynchranously
	errorChan := make(chan error, 1)
	var err error
	err = nil
	var currOPID uint32
	//currOPID = d.IncOPId()
	//var setServerChannel chan bool

	go func() {
		currOPID = d.IncOPId()
	out:

		select {
		case <-d.stopChan:
			//print("VALLALA\n\n\n")
			break out
		// Execution of asynchronous rpc call finishes

		default:

			finishedChanTwo := make(chan *rpc.Call, 1)

			//for loop to buffer until opid condition is matched

			for {
				if d.currSentOpID == currOPID {

					var putRequestSent bool

					ongoingPut.Store(key, true)
					putTracer := tracer.CreateTrace()
					putMessage := Put{
						ClientId: clientId,
						OpId:     currOPID,
						Key:      key,
						Value:    value,
						Token:    nil,
					}

					putTracer.RecordAction(putMessage)
					putMessageRequest := PutRequest{
						ClientId:              clientId,
						OpId:                  currOPID,
						Key:                   key,
						Value:                 value,
						LocalTailServerIPPort: d.localTailServerIPPort,
						Token:                 putTracer.GenerateToken(),
					}
					// Keeps looping
					tailProblem := false
					//var call *rpc.Call
					//call = d.headServerRpcClient.Go("Server.Put", putMessageRequest, &putRequestSent, finishedChanTwo)
					for {
						//print("malarkey\n\n\n\n")
						call := d.headServerRpcClient.Go("Server.Put", putMessageRequest, &putRequestSent, finishedChanTwo)
						//print("Its the headserverclient!\n\n\n")

					o:
						for {
							select {
							case <-finishedChanTwo:
								break o
							default:

							}
						}

						if call.Error != nil {
							var finishChan chan bool
							fmt.Printf("Connection to head failed\n\n\n")

						off:

							for {
								if len(d.changeHeadChan) > 0 {
									continue
								} else {
									if len(d.changeHeadChan) == 0 {
										//

										finishChan = make(chan bool, 1)
										d.changeHeadChan <- true
										d.updateServerConnection("head", finishChan)
										for {
											select {
											case <-finishChan:
												//	print("2\n")
												break off
											default:

											}

										}

									}
								}

							}

							//call = d.headServerRpcClient.Go("Server.Put", putMessageRequest, &putRequestSent, finishedChanTwo)

						} else if tailProblem {
							//print("\n\n3\n\n")
							var finishChan chan bool
							//print("ENTER2\n")

						down:
							for {

								if len(d.changeTailChan) > 0 {
									continue
								} else {
									if len(d.changeTailChan) == 0 {
										//
										//print("ENTER3\n")
										finishChan = make(chan bool, 1)
										d.changeTailChan <- true
										d.updateServerConnection("tail", finishChan)

										for {
											select {
											case <-finishChan:

												break down
											default:

											}

										}

									}
								}

							}
							//print("Replacing tail server connection\n\n\n")
							//call = d.headServerRpcClient.Go("Server.Put", putMessageRequest, &putRequestSent, finishedChanTwo)

						}

						//print("IT TIMED OUT\n")
					restart:
						for timeout := time.After(1 * time.Second); ; {
							//print("4\n")
							//This loop is for listening to the put response from tail server, using the d instance variable putReceivedChan
							//print(putMessage.OpId)
							select {
							case <-d.stopChan:
								//print("FFASD\n\n\n\n" + d.clientID)
								break out
							case <-timeout:
								tailProblem = true
								break restart
							case putReceivedStruct := <-d.putReceivedChan:
								//print("FASFA\n\n\n\n" + d.clientID)
								receivedTrace := tracer.ReceiveToken(putReceivedStruct.Token)
								receivedTrace.RecordAction(PutResultRecvd{
									OpId: putReceivedStruct.OpId,
									GId:  putReceivedStruct.GId,
									Key:  putReceivedStruct.Key,
								})
								putResultStruct := ResultStruct{
									OpId:   putReceivedStruct.OpId,
									GId:    putReceivedStruct.GId,
									Result: putReceivedStruct.Value,
								}
								d.notifyCh <- putResultStruct

								ongoingPut.Delete(key)
								d.mu.Lock()
								d.currSentOpID = d.currSentOpID + 1
								d.mu.Unlock()
								//print("DONEZO\n\n")
								//print(d.currSentOpID)
								break out

							default:
								//print("alwayshere")
							}
						}
						//fail = true

					}

				}

			}
		}

	}()
	select {
	case putError := <-errorChan:
		err = putError
		break
	default:
		break
	}
	return currOPID, err
}

// Stop Stops the KVS instance from communicating with the KVS and from delivering any results via the notify-channel.
// This call always succeeds.
func (d *KVS) Stop() {
	// End all connections in the struct
	d.stopChan <- true

	//d.coordTCPConn.Close()
	if d.localTailListener != nil {
		d.localTailListener.Close()
	}
	if d.coordRpcClient != nil {
		d.coordRpcClient.Close()
	}
	if d.headServerRpcClient != nil {
		d.headServerRpcClient.Close()
	}
	if d.tailServerRpcClient != nil {
		d.tailServerRpcClient.Close()
	}
	if d.localTailListener != nil {
		d.localTailListener.Close()
	}
	kvsTracer := d.kvsTracer.CreateTrace()
	kvsTracer.RecordAction(KvslibStop{
		ClientId: d.clientID,
	})

}

// Method for server to call to confirm that the putResult went through and to input it into channel

func (d *KVS) ReceivePutResult(putResponse PutResult, putSucceed *bool) error {

	fmt.Printf("Received %v", putResponse)

	d.putReceivedChan <- putResponse
	return nil
}

func (d *KVS) ReceiveGetResult(putResponse PutResult, putSucceed *bool) error {

	fmt.Printf("Received %v", putResponse)

	d.putReceivedChan <- putResponse
	return nil
}

func (d *KVS) updateServerConnection(serverRequest string, finishChan chan bool) error {
	//print("OMEGA/n/n/n")
out:
	for {

		var reply HeadTailServerReply
		var requestStruct HeadTailServerRequest
		kvsTracer := d.kvsTracer.CreateTrace()
		//getServerChan := make(chan *rpc.Call, 1)

		var receiveCall *rpc.Call
		buffer := false
		for {
			if !buffer {
				if serverRequest == "tail" {
					kvsTracer.RecordAction(TailReq{
						ClientId: d.clientID,
					})
					requestStruct = HeadTailServerRequest{
						ClientId: d.clientID,
						Type:     "tail",
						Token:    kvsTracer.GenerateToken(),
					}
				} else {
					kvsTracer.RecordAction(HeadReq{
						ClientId: d.clientID,
					})
					requestStruct = HeadTailServerRequest{
						ClientId: d.clientID,
						Type:     "head",
						Token:    kvsTracer.GenerateToken(),
					}
				}
			}

			//print("d\n")s
			//print("doggo\n\n\n\n\n")
			receiveCall = d.coordRpcClient.Go("ClientLearnServers.GetHeadTailServer", requestStruct, &reply, nil)
		b:
			for {
				//print("jason\n\n\n\n\n")
				select {
				case <-receiveCall.Done:
					if receiveCall.Error != nil {
						//print("bourne\n\n\n\n\n")
						time.Sleep(300 * time.Millisecond)
						buffer = true
						break b
					} else {

						kTracer := d.kvsTracer.ReceiveToken(reply.Token)

						if serverRequest == "tail" {
							//print("ratta\n\n\n\n\n")
							kTracer.RecordAction(TailResRecvd{
								ClientId: d.clientID,
								ServerId: reply.ServerId,
							})
							var raddr *net.TCPAddr
							raddr, _ = net.ResolveTCPAddr("tcp", reply.TailServerAddress)
							openPort, err := getFreePort("127.0.0.1")
							var laddr *net.TCPAddr
							for {
								if err != nil {
									openPort, err = getFreePort("127.0.0.1")
								} else {
									laddr, _ = net.ResolveTCPAddr("tcp", openPort)
									break
								}

							}

							for {
								//print("rogotto\n\n\n\n\n")

								conn, err := net.DialTCP("tcp", laddr, raddr)
								if err != nil {
									//print("BROKEN\n\n")
									time.Sleep(300 * time.Millisecond)
									break b

								} else {
									d.tailServerRpcClient = rpc.NewClient(conn)
									break
								}

							}

						N:
							for {
								select {
								case <-d.changeTailChan:
								default:
									break N
								}
							}
							//d.changeTailChan = make(chan bool, 1)

							break out

						} else {
							kTracer.RecordAction(HeadResRecvd{
								ClientId: d.clientID,
								ServerId: reply.ServerId,
							})
							var raddr *net.TCPAddr
							raddr, _ = net.ResolveTCPAddr("tcp", reply.HeadServerAddress)
							openPort, err := getFreePort("127.0.0.1")
							var laddr *net.TCPAddr
							for {
								if err != nil {
									openPort, err = getFreePort("127.0.0.1")
								} else {
									laddr, _ = net.ResolveTCPAddr("tcp", openPort)
									break
								}

							}
							for {
								conn, err := net.DialTCP("tcp", laddr, raddr)
								if err != nil {
									// loop
									//print("HOROVEEN\n\n\n\n")
									break b

								} else {
									d.headServerRpcClient = rpc.NewClient(conn)
									break

								}

							}
							//print("\nExecuted\n")

							//print("clear channel \n\n\n\n")
						L:
							for {
								select {
								case <-d.changeHeadChan:
								default:
									break L
								}
							}
							//d.changeHeadChan = make(chan bool, 1)
							//print("done clearing channel \n\n\n\n")
							break out

						}

					}
				}
			}
			//print("j\n")

		}
	}
	if finishChan != nil {
		finishChan <- true
	}

	//print("Found new address\n\n")

	return nil
}

func getFreePort(ip string) (string, error) {
	ip = ip + ":0"
	addr, err := net.ResolveUDPAddr("udp", ip)
	if err != nil {
		return "", err
	}

	l, err := net.ListenUDP("udp", addr)
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.LocalAddr().String(), nil
	// return l.Addr().(*net.TCPAddr).Port, nil
}

func (d *KVS) IncOPId() uint32 {
	d.muTwo.Lock()
	defer d.muTwo.Unlock()
	d.currOpID = d.currOpID + 1
	returnID := d.currOpID
	return returnID
}

func (c *CoordNotification) NotifyClientForServerFailure(request string, reply *bool) error {
	fmt.Println("Coord says this server failed: ", request)
	*reply = true
	return nil
}

func startListeningForCoord(coordListenAddr string, c *CoordNotification) error {
	//localAddr, err := net.ResolveUDPAddr("udp", coordListenAddr)
	//if err != nil {
	//	fmt.Printf("Could not start client;failed to resolve addr: %v\nExiting", err.Error())
	//	os.Exit(2)
	//}
	//localIp := localAddr.IP.String()
	//ipPort, err := getFreePort(localIp)
	//if err != nil {
	//	fmt.Printf("Could not get free ipPort for client: %v\nExiting", err)
	//	os.Exit(2)
	//}

	coordlistenAddr, err := net.ResolveTCPAddr("tcp", coordListenAddr)
	if err != nil {
		return errors.New("client could not resolve coordListenAddr: " + err.Error())
	}

	fmt.Println("listening for coord msgs at: ", coordlistenAddr.String())

	inbound, err := net.ListenTCP("tcp", coordlistenAddr)
	if err != nil {
		return errors.New("unable to listen inbound coord connections: " + err.Error())
	}

	err = rpc.Register(c)
	if err != nil {
		return errors.New("could not register ClientLearnServers type for RPC: " + err.Error())
	}

	go func() {
		for {
			fmt.Println("waiting for incoming coord connections")
			rpc.Accept(inbound)
			fmt.Println("go routine test test")
		}
	}()

	return nil
}
